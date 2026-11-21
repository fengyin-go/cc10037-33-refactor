"""质量计数的统一口径与可核对账本。

本模块把「质量门禁」和「质量执行报告」原本各写一遍的计数流程抽出来，
并明确区分两种计量单位：

``RULE_ROWS``
    逐规则的行计量。分子是该规则认定的坏行数，分母是该规则实际校验的行数。
    两条规则的样本即使完全重叠，也各自拥有独立的分母，不能相加成「两次执行」。

``EXECUTION_BATCH``
    执行批次计量。一次执行无论包含多少条规则，都只计一次执行。

空分母与比例精度沿用重构前的约定：

* 行比例：``failed / (total_rows or 1)``，保留 4 位小数；
* 单批次通过率：``passed / max(total, 1) * 100``，保留 2 位小数；
* 表级行通过率：``sum(passed) / max(sum(total), 1) * 100``，保留 1 位小数。

交接边界（刻意写死在账本里，避免调用方各自猜测）：

* 只有一次执行的全部规则都进入终态，执行才算「完成」；
* 只要还有规则在计算，逐规则结果只能作为诊断计数，不得交给门禁；
* 只要有规则失败，本次执行不算「完整执行」，不写归档记录，需以新的执行编号重试；
* 只有完整执行才会写且只写一条归档记录；重试产生的新执行不会与旧执行累加。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Iterable, Sequence

ROW_UNIT = "RULE_ROWS"
EXECUTION_UNIT = "EXECUTION_BATCH"

GATE_RATIO_PRECISION = 4
BATCH_PASS_RATE_PRECISION = 2
TABLE_PASS_RATE_PRECISION = 1

#: 只有这些状态代表一次执行已有结论，可以进入表级「已确认执行次数」。
CONFIRMED_BATCH_STATUSES = frozenset({"已通过", "未通过", "已阻断"})


def timestamp() -> str:
    """统一的时间戳格式，和原实现一样使用本地时间。"""
    return datetime.now().isoformat()


def row_denominator(total_rows: int) -> int:
    """空分母沿用原约定：0 行按 1 处理。"""
    return int(total_rows) or 1


def row_ratio(
    failed_rows: int, total_rows: int, precision: int = GATE_RATIO_PRECISION
) -> float:
    """行计量比例，精度与门禁原口径一致。"""
    return round(int(failed_rows) / row_denominator(total_rows), precision)


def percent_rate(numerator: int, denominator: int, precision: int) -> float:
    """百分比口径，空分母按 1 处理，与执行报告原实现一致。"""
    return round(int(numerator) / max(int(denominator), 1) * 100, precision)


def count_signature(payload: dict[str, Any]) -> str:
    """给计数数据生成可复核的短签名（不包含时间戳，重算稳定）。"""
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


class RuleCountStatus(str, Enum):
    """单条规则诊断的终态。"""

    COMPLETED = "COMPLETED"  # 规则算完，产出可核对的行计数
    FAILED = "FAILED"        # 规则计算失败/中止，只能留诊断，不提供门禁分母


class ExecutionStatus(str, Enum):
    """一次执行的状态。"""

    OPEN = "OPEN"              # 仍有规则在计算，只能给诊断计数
    COMPLETED = "COMPLETED"    # 全部规则终态且无失败，可交门禁、可归档
    FAILED = "FAILED"          # 全部规则终态但至少一条失败，需重试
    ARCHIVED = "ARCHIVED"      # 已写入归档记录


@dataclass(frozen=True)
class RuleRowCount:
    """一条规则对一批数据的可核对行计数（计量单位：``RULE_ROWS``）。"""

    execution_id: str
    rule_id: str
    total_rows: int
    passed_rows: int
    failed_rows: int
    status: RuleCountStatus
    confirmed_at: str | None = None
    error: str | None = None
    # 原引擎在「未知校验类型」等分支会给出与 failed/total 不一致的比例，
    # 这里保留原值以保证门禁百分比完全不变。
    reported_failed_ratio: float | None = None

    def __post_init__(self) -> None:
        # 逐规则诊断各自保留确认时点；失败规则记录的是失败确认时刻。
        if self.confirmed_at is None:
            object.__setattr__(self, "confirmed_at", timestamp())

    @property
    def unit(self) -> str:
        return ROW_UNIT

    @property
    def denominator(self) -> int:
        """该规则自己的分母，空分母按 1。"""
        return row_denominator(self.total_rows)

    @property
    def failed_ratio(self) -> float:
        if self.reported_failed_ratio is not None:
            return self.reported_failed_ratio
        return self.failed_rows / self.denominator

    @property
    def is_terminal(self) -> bool:
        return self.status in (RuleCountStatus.COMPLETED, RuleCountStatus.FAILED)

    @property
    def counts_toward_gate(self) -> bool:
        """失败/中止规则的计数不能成为门禁分母。"""
        return self.status == RuleCountStatus.COMPLETED

    def to_dict(self) -> dict[str, Any]:
        return {
            "unit": self.unit,
            "execution_id": self.execution_id,
            "rule_id": self.rule_id,
            "total_rows": self.total_rows,
            "passed_rows": self.passed_rows,
            "failed_rows": self.failed_rows,
            "denominator": self.denominator,
            "failed_ratio": round(self.failed_ratio, GATE_RATIO_PRECISION),
            "status": self.status.value,
            "confirmed_at": self.confirmed_at,
            "error": self.error,
        }

    @classmethod
    def from_result(
        cls,
        execution_id: str,
        result: Any,
        confirmed_at: str | None = None,
    ) -> "RuleRowCount":
        """从 ``ValidationResult`` 生成行计数，保留原始比例。"""
        return cls(
            execution_id=execution_id,
            rule_id=str(result.rule_id),
            total_rows=int(result.total_rows),
            passed_rows=int(result.passed_rows),
            failed_rows=int(result.failed_rows),
            status=RuleCountStatus.COMPLETED,
            confirmed_at=confirmed_at or timestamp(),
            reported_failed_ratio=float(result.failed_ratio),
        )


@dataclass(frozen=True)
class GateCounting:
    """门禁可消费的可核对计数（计量单位：``RULE_ROWS``）。"""

    execution_id: str
    rule_counts: tuple[RuleRowCount, ...]
    included_rule_ids: tuple[str, ...]
    excluded_rule_ids: tuple[str, ...]
    total_failed_rows: int
    denominator: int
    failed_ratio: float
    precision: int = GATE_RATIO_PRECISION
    unit: str = ROW_UNIT

    def to_dict(self) -> dict[str, Any]:
        return {
            "unit": self.unit,
            "execution_id": self.execution_id,
            "included_rule_ids": list(self.included_rule_ids),
            "excluded_rule_ids": list(self.excluded_rule_ids),
            "total_failed_rows": self.total_failed_rows,
            "denominator": self.denominator,
            "failed_ratio": self.failed_ratio,
            "precision": self.precision,
            "rule_counts": [count.to_dict() for count in self.rule_counts],
        }


def build_gate_counting(
    execution_id: str,
    counts: Sequence[RuleRowCount],
    precision: int = GATE_RATIO_PRECISION,
) -> GateCounting:
    """按原门禁口径汇总行计数：``sum(failed) / (sum(total) or 1)``。"""
    countable = [count for count in counts if count.counts_toward_gate]
    excluded = [count for count in counts if not count.counts_toward_gate]
    total_failed = sum(count.failed_rows for count in countable)
    denominator = sum(count.total_rows for count in countable) or 1
    ratio = round(total_failed / denominator, precision) if countable else 0.0
    return GateCounting(
        execution_id=execution_id,
        rule_counts=tuple(countable),
        included_rule_ids=tuple(count.rule_id for count in countable),
        excluded_rule_ids=tuple(count.rule_id for count in excluded),
        total_failed_rows=total_failed,
        denominator=denominator,
        failed_ratio=ratio,
        precision=precision,
    )


@dataclass(frozen=True)
class ExecutionArchiveRecord:
    """一次完整执行的计数归档（计量单位：``EXECUTION_BATCH``）。"""

    archive_id: str
    execution_id: str
    pipeline_id: str
    attempt: int
    retry_of: str | None
    rule_counts: tuple[RuleRowCount, ...]
    included_rule_ids: tuple[str, ...]
    execution_count: int
    total_failed_rows: int
    denominator: int
    failed_ratio: float
    archived_at: str
    count_signature: str
    unit: str = EXECUTION_UNIT

    def to_dict(self) -> dict[str, Any]:
        return {
            "unit": self.unit,
            "archive_id": self.archive_id,
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "attempt": self.attempt,
            "retry_of": self.retry_of,
            "execution_count": self.execution_count,
            "included_rule_ids": list(self.included_rule_ids),
            "total_failed_rows": self.total_failed_rows,
            "denominator": self.denominator,
            "failed_ratio": self.failed_ratio,
            "archived_at": self.archived_at,
            "count_signature": self.count_signature,
            "rule_counts": [count.to_dict() for count in self.rule_counts],
        }


class ExecutionLedgerError(RuntimeError):
    """计数账本的基类错误。"""


class UnknownExecutionError(ExecutionLedgerError, KeyError):
    """引用了未登记的执行编号。"""


class IncompleteExecutionError(ExecutionLedgerError):
    """执行尚未完成，不能写归档记录。"""


class ExecutionFailedError(ExecutionLedgerError):
    """执行含失败规则，不是完整执行，不能写归档记录。"""


class NotGateEligibleError(ExecutionLedgerError):
    """当前执行不能作为门禁依据。"""


@dataclass
class ExecutionRecord:
    """计数账本中的一次执行（可变状态只在本模块内维护）。"""

    execution_id: str
    pipeline_id: str
    expected_rule_ids: tuple[str, ...] = ()
    attempt: int = 1
    retry_of: str | None = None
    rule_counts: dict[str, RuleRowCount] = field(default_factory=dict)
    status: ExecutionStatus = ExecutionStatus.OPEN
    started_at: str = field(default_factory=timestamp)
    finalized_at: str | None = None
    archived_at: str | None = None
    archive_record: ExecutionArchiveRecord | None = None

    @property
    def ordered_counts(self) -> tuple[RuleRowCount, ...]:
        """按规则登记顺序返回，保留调用方纳入的规则编号顺序。"""
        return tuple(
            self.rule_counts[rule_id]
            for rule_id in self.expected_rule_ids
            if rule_id in self.rule_counts
        )

    @property
    def terminal_rule_ids(self) -> tuple[str, ...]:
        return tuple(
            rule_id
            for rule_id in self.expected_rule_ids
            if rule_id in self.rule_counts and self.rule_counts[rule_id].is_terminal
        )

    @property
    def pending_rule_ids(self) -> tuple[str, ...]:
        return tuple(
            rule_id
            for rule_id in self.expected_rule_ids
            if rule_id not in self.rule_counts or not self.rule_counts[rule_id].is_terminal
        )

    @property
    def failed_rule_ids(self) -> tuple[str, ...]:
        return tuple(
            rule_id
            for rule_id in self.expected_rule_ids
            if rule_id in self.rule_counts
            and self.rule_counts[rule_id].status == RuleCountStatus.FAILED
        )

    @property
    def all_rules_terminal(self) -> bool:
        return all(
            rule_id in self.rule_counts and self.rule_counts[rule_id].is_terminal
            for rule_id in self.expected_rule_ids
        )

    def refresh_status(self) -> ExecutionStatus:
        """根据规则终态刷新执行状态；已归档的执行不再回退。"""
        if self.status == ExecutionStatus.ARCHIVED:
            return self.status
        if not self.all_rules_terminal:
            self.status = ExecutionStatus.OPEN
        elif self.failed_rule_ids:
            self.status = ExecutionStatus.FAILED
        else:
            self.status = ExecutionStatus.COMPLETED
        return self.status

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "attempt": self.attempt,
            "retry_of": self.retry_of,
            "status": self.status.value,
            "expected_rule_ids": list(self.expected_rule_ids),
            "terminal_rule_ids": list(self.terminal_rule_ids),
            "pending_rule_ids": list(self.pending_rule_ids),
            "failed_rule_ids": list(self.failed_rule_ids),
            "started_at": self.started_at,
            "finalized_at": self.finalized_at,
            "archived_at": self.archived_at,
            "archive_id": self.archive_record.archive_id if self.archive_record else None,
        }


class QualityCountLedger:
    """质量计数账本：登记规则诊断、判定交接边界、写执行归档。"""

    def __init__(self) -> None:
        self._executions: dict[str, ExecutionRecord] = {}
        self._archive: list[ExecutionArchiveRecord] = []

    # ---- 登记 ----

    def begin(
        self,
        execution_id: str,
        pipeline_id: str,
        rule_ids: Iterable[str],
        *,
        attempt: int = 1,
        retry_of: str | None = None,
    ) -> ExecutionRecord:
        """登记一次执行；重试必须使用新的执行编号。"""
        if execution_id in self._executions:
            raise ExecutionLedgerError(
                f"执行编号 {execution_id} 已存在；重试请登记新的执行编号，"
                "不要把不同轮次的规则计数覆盖到同一次执行上"
            )
        if retry_of is not None and retry_of not in self._executions:
            raise UnknownExecutionError(retry_of)
        record = ExecutionRecord(
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            expected_rule_ids=tuple(dict.fromkeys(str(rule_id) for rule_id in rule_ids)),
            attempt=attempt,
            retry_of=retry_of,
        )
        self._executions[execution_id] = record
        return record

    def get(self, execution_id: str) -> ExecutionRecord:
        try:
            return self._executions[execution_id]
        except KeyError as exc:
            raise UnknownExecutionError(execution_id) from exc

    def record_rule_count(
        self, execution_id: str, count: RuleRowCount
    ) -> ExecutionRecord:
        """登记一条规则的终态计数；同一执行内不允许覆盖。"""
        state = self.get(execution_id)
        if count.rule_id not in state.expected_rule_ids:
            raise ExecutionLedgerError(
                f"规则 {count.rule_id} 未在该执行 {execution_id} 的登记范围内"
            )
        if count.rule_id in state.rule_counts:
            raise ExecutionLedgerError(
                f"执行 {execution_id} 的规则 {count.rule_id} 已有终态计数；"
                "重试请使用新的执行编号，避免把同一轮样本覆盖当成两次执行"
            )
        state.rule_counts[count.rule_id] = count
        state.refresh_status()
        return state

    def record_result(
        self,
        execution_id: str,
        result: Any,
        confirmed_at: str | None = None,
    ) -> RuleRowCount:
        count = RuleRowCount.from_result(execution_id, result, confirmed_at)
        self.record_rule_count(execution_id, count)
        return count

    def record_rule_failure(
        self,
        execution_id: str,
        rule_id: str,
        error: str,
        confirmed_at: str | None = None,
    ) -> RuleRowCount:
        """把失败/中止的规则记为终态诊断，但不提供门禁分母。"""
        count = RuleRowCount(
            execution_id=execution_id,
            rule_id=rule_id,
            total_rows=0,
            passed_rows=0,
            failed_rows=0,
            status=RuleCountStatus.FAILED,
            confirmed_at=confirmed_at or timestamp(),
            error=error,
        )
        self.record_rule_count(execution_id, count)
        return count

    # ---- 边界判定 ----

    def handoff_decision(self, execution_id: str) -> dict[str, Any]:
        """说明诊断计数能否交接给门禁，以及纳入了哪些规则编号。

        * ``OPEN``：仍有规则在计算，只能作为诊断，不得交门禁；
        * ``FAILED``：本次执行不完整，不写归档，应换新执行编号重试；
        * ``COMPLETED`` / ``ARCHIVED``：可交门禁，且已完成归档。
        """
        state = self.get(execution_id)
        state.refresh_status()
        counting = build_gate_counting(execution_id, state.ordered_counts)

        eligible = state.status in (
            ExecutionStatus.COMPLETED,
            ExecutionStatus.ARCHIVED,
        )
        if state.status == ExecutionStatus.OPEN:
            reason = (
                "执行尚未完成：规则 "
                + "、".join(state.pending_rule_ids)
                + " 仍在计算，诊断计数不得作为门禁依据"
            )
        elif state.status == ExecutionStatus.FAILED:
            reason = (
                "执行不完整：规则 "
                + "、".join(state.failed_rule_ids)
                + " 计算失败，本次执行不归档，需换新执行编号重试"
            )
        else:
            reason = ""

        return {
            "execution_id": execution_id,
            "status": state.status.value,
            "eligible": eligible,
            "archive_allowed": eligible,
            "reason": reason,
            "included_rule_ids": list(counting.included_rule_ids),
            "excluded_rule_ids": list(counting.excluded_rule_ids),
            "diagnostic_rule_ids": list(state.terminal_rule_ids),
            "pending_rule_ids": list(state.pending_rule_ids),
            "counting": counting.to_dict() if eligible else None,
            "diagnostic_counting": counting.to_dict(),
        }

    def handoff_to_gate(self, execution_id: str) -> GateCounting:
        """返回门禁可消费的计数；未完成或失败的执行直接拒绝。"""
        decision = self.handoff_decision(execution_id)
        if not decision["eligible"]:
            raise NotGateEligibleError(decision["reason"])
        state = self.get(execution_id)
        return build_gate_counting(execution_id, state.ordered_counts)

    # ---- 归档 ----

    def finalize(self, execution_id: str) -> ExecutionRecord:
        """收口一次执行；只有完整执行才会写且只写一条归档记录。"""
        state = self.get(execution_id)
        state.refresh_status()
        if state.status == ExecutionStatus.OPEN:
            raise IncompleteExecutionError(
                "执行尚未完成，规则 "
                + "、".join(state.pending_rule_ids)
                + " 仍在计算，批次不能计入归档"
            )
        if state.status == ExecutionStatus.FAILED:
            # 失败的执行不是完整执行，不写归档；重试须登记新的执行编号。
            raise ExecutionFailedError(
                "执行不完整，规则 "
                + "、".join(state.failed_rule_ids)
                + " 计算失败，本次执行不归档；请换新执行编号重试"
            )
        if state.finalized_at is None:
            state.finalized_at = timestamp()
        if state.status == ExecutionStatus.COMPLETED and state.archive_record is None:
            self._write_archive(state)
        return state

    def _write_archive(self, state: ExecutionRecord) -> ExecutionArchiveRecord:
        counting = build_gate_counting(state.execution_id, state.ordered_counts)
        signature = count_signature({
            "execution_id": state.execution_id,
            "rule_ids": list(counting.included_rule_ids),
            "counts": [
                [count.rule_id, count.total_rows, count.passed_rows, count.failed_rows]
                for count in counting.rule_counts
            ],
        })
        record = ExecutionArchiveRecord(
            archive_id=f"ARC-{state.execution_id}",
            execution_id=state.execution_id,
            pipeline_id=state.pipeline_id,
            attempt=state.attempt,
            retry_of=state.retry_of,
            rule_counts=counting.rule_counts,
            included_rule_ids=counting.included_rule_ids,
            execution_count=1,  # 一次执行只计一次，与规则条数无关
            total_failed_rows=counting.total_failed_rows,
            denominator=counting.denominator,
            failed_ratio=counting.failed_ratio,
            archived_at=timestamp(),
            count_signature=signature,
        )
        state.archive_record = record
        state.archived_at = record.archived_at
        state.status = ExecutionStatus.ARCHIVED
        self._archive.append(record)
        return record

    def archive_records(self) -> tuple[ExecutionArchiveRecord, ...]:
        return tuple(self._archive)

    def archived_execution_count(self, pipeline_id: str | None = None) -> int:
        """归档表分母：按执行编号计数，不按规则条数计数。"""
        return sum(
            record.execution_count
            for record in self._archive
            if pipeline_id is None or record.pipeline_id == pipeline_id
        )

    def archived_execution_ids(self, pipeline_id: str | None = None) -> tuple[str, ...]:
        return tuple(
            record.execution_id
            for record in self._archive
            if pipeline_id is None or record.pipeline_id == pipeline_id
        )


# ---------------------------------------------------------------------------
# 执行报告（表级）计数
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExecutionBatchCount:
    """一条校验批次的可核对计数（计量单位：``EXECUTION_BATCH``）。"""

    execution_id: str
    table: str
    total_rows: int
    passed_rows: int
    failed_rows: int
    status: str
    confirmed: bool
    confirmed_at: str | None = None

    @property
    def unit(self) -> str:
        return EXECUTION_UNIT

    @property
    def denominator(self) -> int:
        return row_denominator(self.total_rows)

    @property
    def pass_rate_pct(self) -> float:
        """单批次通过率沿用原口径：2 位小数、空分母按 1。"""
        return percent_rate(
            self.passed_rows, self.total_rows, BATCH_PASS_RATE_PRECISION
        )

    @property
    def fail_rate_pct(self) -> float:
        return percent_rate(
            self.failed_rows, self.total_rows, BATCH_PASS_RATE_PRECISION
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "unit": self.unit,
            "execution_id": self.execution_id,
            "table": self.table,
            "total_rows": self.total_rows,
            "passed_rows": self.passed_rows,
            "failed_rows": self.failed_rows,
            "denominator": self.denominator,
            "pass_rate_pct": self.pass_rate_pct,
            "status": self.status,
            "confirmed": self.confirmed,
            "confirmed_at": self.confirmed_at,
        }


def collect_execution_batches(
    rows: Iterable[dict[str, Any]],
    confirmed_statuses: frozenset[str] = CONFIRMED_BATCH_STATUSES,
) -> list[ExecutionBatchCount]:
    """把批次记录转成可核对计数。

    同一个批次编号只计一次。即使一批数据上跑了多条规则（或重试写入了重复行），
    也只代表一次执行，不能把规则样本覆盖累加成多次执行。
    """
    batches: list[ExecutionBatchCount] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        execution_id = str(
            row.get("批次标识")
            or row.get("execution_id")
            or f"row-{row.get('id', index)}"
        )
        if execution_id in seen:
            continue
        seen.add(execution_id)
        status = str(row.get("status") or row.get("处理状态") or "")
        confirmed_at = row.get("确认时点") or row.get("last_operated_at")
        batches.append(ExecutionBatchCount(
            execution_id=execution_id,
            table=str(row.get("目标表", "未知")),
            total_rows=int(row.get("总行数", "0") or 0),
            passed_rows=int(row.get("通过行数", "0") or 0),
            failed_rows=int(row.get("失败行数", "0") or 0),
            status=status,
            confirmed=status in confirmed_statuses,
            confirmed_at=str(confirmed_at) if confirmed_at else None,
        ))
    return batches


@dataclass(frozen=True)
class TableCounting:
    """一张表的执行报告计数：行计量与执行批次计量同时保留。"""

    table: str
    batches: tuple[ExecutionBatchCount, ...]
    total_rows: int
    passed_rows: int
    failed_rows: int
    row_pass_rate_pct: float
    execution_ids: tuple[str, ...]
    confirmed_execution_ids: tuple[str, ...]
    passed_execution_ids: tuple[str, ...]
    confirmed_execution_count: int
    passed_execution_count: int
    execution_pass_rate_pct: float

    @property
    def execution_count(self) -> int:
        return len(self.batches)

    @property
    def row_unit(self) -> str:
        return ROW_UNIT

    @property
    def execution_unit(self) -> str:
        return EXECUTION_UNIT

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "unit_row": self.row_unit,
            "unit_execution": self.execution_unit,
            "execution_count": self.execution_count,
            "execution_ids": list(self.execution_ids),
            "confirmed_execution_count": self.confirmed_execution_count,
            "confirmed_execution_ids": list(self.confirmed_execution_ids),
            "passed_execution_count": self.passed_execution_count,
            "passed_execution_ids": list(self.passed_execution_ids),
            "total_rows": self.total_rows,
            "passed_rows": self.passed_rows,
            "failed_rows": self.failed_rows,
            "row_pass_rate_pct": self.row_pass_rate_pct,
            "execution_pass_rate_pct": self.execution_pass_rate_pct,
            "batches": [batch.to_dict() for batch in self.batches],
        }


def build_table_counting(
    table: str, batches: Sequence[ExecutionBatchCount]
) -> TableCounting:
    """表级汇总：行通过率沿用原口径，执行通过率按已确认执行次数判断。"""
    total_rows = sum(batch.total_rows for batch in batches)
    passed_rows = sum(batch.passed_rows for batch in batches)
    failed_rows = sum(batch.failed_rows for batch in batches)

    confirmed = [batch for batch in batches if batch.confirmed]
    passed_executions = [batch for batch in confirmed if batch.status == "已通过"]

    return TableCounting(
        table=table,
        batches=tuple(batches),
        total_rows=total_rows,
        passed_rows=passed_rows,
        failed_rows=failed_rows,
        row_pass_rate_pct=percent_rate(
            passed_rows, total_rows, TABLE_PASS_RATE_PRECISION
        ),
        execution_ids=tuple(batch.execution_id for batch in batches),
        confirmed_execution_ids=tuple(batch.execution_id for batch in confirmed),
        passed_execution_ids=tuple(batch.execution_id for batch in passed_executions),
        confirmed_execution_count=len(confirmed),
        passed_execution_count=len(passed_executions),
        execution_pass_rate_pct=percent_rate(
            len(passed_executions), len(confirmed), TABLE_PASS_RATE_PRECISION
        ),
    )


def build_table_countings(
    batches: Sequence[ExecutionBatchCount],
) -> dict[str, TableCounting]:
    """按表分组，保持首次出现的顺序。"""
    grouped: dict[str, list[ExecutionBatchCount]] = {}
    for batch in batches:
        grouped.setdefault(batch.table, []).append(batch)
    return {
        table: build_table_counting(table, group)
        for table, group in grouped.items()
    }
