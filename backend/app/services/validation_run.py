"""校验批次领域服务：执行质量校验批次、产出校验报告和异常标记。

上游：quality_rule 产出校验规则 → validation_run 执行校验批次
消费：alert_policy 消费校验结果触发告警
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.engine.quality_counting import (
    build_table_countings,
    collect_execution_batches,
    timestamp,
)
from app.services.base import BaseModuleService
from app.store import store


def _partition_of(batch_id: str) -> Any:
    """取批次记录上的校验分区（计数时不参与计量，仅用于展示）。"""
    for row in store.rows("validation_run"):
        if str(row.get("批次标识")) == batch_id:
            return row.get("校验分区")
    return None


class ValidationRunService(BaseModuleService):
    MODULE = "validation_run"
    ENTITY = "校验批次"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["批次标识", "目标表", "校验分区", "总行数", "通过行数", "失败行数", "校验耗时"]
    REQUIRED_FIELDS = ["批次标识", "目标表", "校验分区"]
    STATUS_ORDER = ["校验中", "已通过", "未通过", "已阻断"]
    ACTION_RULES = {"确认通过": "校验中", "标记阻断": "未通过"}
    NEGATIVE_ACTIONS = []

    def query_by_table(self, table_name: str) -> list[dict[str, Any]]:
        """按目标表查询校验批次记录。

        从 store 中的校验批次数据里筛选指定表的批次，
        返回批次列表供质量趋势分析消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if table_name.casefold() in str(row.get("目标表", "")).casefold():
                result.append(dict(row))
        return result

    def compute_pass_rate(self, batch_id: str) -> dict[str, Any]:
        """计算校验批次的通过率。

        基于批次记录中的总行数和通过行数字段，计算通过率、失败率等核心质量指标。
        计数统一走 :func:`collect_execution_batches`，空分母与精度保持原口径。
        """
        batches = collect_execution_batches(store.rows(self.MODULE))
        batch = next(
            (item for item in batches if item.execution_id == batch_id), None
        )
        if batch is None:
            return {"error": f"批次 {batch_id} 不存在"}

        return {
            "batch_id": batch.execution_id,
            "table": batch.table,
            "partition": _partition_of(batch_id),
            "total_rows": batch.total_rows,
            "passed_rows": batch.passed_rows,
            "failed_rows": batch.failed_rows,
            "pass_rate_pct": batch.pass_rate_pct,
            "fail_rate_pct": batch.fail_rate_pct,
            "status": "通过" if batch.pass_rate_pct >= 99.9 else "需关注",
            "counting": batch.to_dict(),
            "computed_at": timestamp(),
        }

    def aggregate_by_table(self) -> dict[str, Any]:
        """按目标表汇总校验批次统计。

        行通过率沿用原口径（累计通过行 / 累计总行）。
        执行通过率按「已确认的执行次数」判断：同一批次编号只计一次执行，
        校验中/未收口的不进入执行次数分母。
        """
        batches = collect_execution_batches(store.rows(self.MODULE))
        countings = build_table_countings(batches)

        table_stats: dict[str, dict[str, Any]] = {}
        for table, counting in countings.items():
            table_stats[table] = {
                "批次总数": counting.execution_count,
                "通过批次": sum(
                    1 for b in counting.batches if b.status == "已通过"
                ),
                "失败批次": sum(
                    1 for b in counting.batches if b.status == "未通过"
                ),
                "阻断批次": sum(
                    1 for b in counting.batches if b.status == "已阻断"
                ),
                "累计总行": counting.total_rows,
                "累计通过": counting.passed_rows,
                "累计失败": counting.failed_rows,
                "通过率": counting.row_pass_rate_pct,
                # 可核对计数：执行编号与确认状态一并保留
                "计量单位": "EXECUTION_BATCH",
                "执行编号": list(counting.execution_ids),
                "已确认执行数": counting.confirmed_execution_count,
                "已确认执行编号": list(counting.confirmed_execution_ids),
                "执行通过率": counting.execution_pass_rate_pct,
                "counting": counting.to_dict(),
            }

        return {
            "tables": len(table_stats),
            "table_stats": table_stats,
            "archive_denominator": len(batches),
            "snapshot_at": timestamp(),
        }

    def detect_degradation(self, table_name: str, threshold_pct: float = 95.0) -> dict[str, Any]:
        """检测数据质量退化趋势。

        对比最新两个校验批次的通过率，当通过率下降到阈值以下时
        标记质量退化并产出告警建议。
        """
        batches = self.query_by_table(table_name)
        if len(batches) < 2:
            return {"table": table_name, "degradation": "数据不足", "batch_count": len(batches)}

        sorted_batches = sorted(
            batches, key=lambda b: str(b.get("批次标识", "")), reverse=True
        )
        current = sorted_batches[0]
        previous = sorted_batches[1]

        cur_pass = int(current.get("通过行数", "0"))
        cur_total = int(current.get("总行数", "1"))
        prev_pass = int(previous.get("通过行数", "0"))
        prev_total = int(previous.get("总行数", "1"))

        cur_rate = round(cur_pass / max(cur_total, 1) * 100, 2)
        prev_rate = round(prev_pass / max(prev_total, 1) * 100, 2)
        delta = round(cur_rate - prev_rate, 2)

        is_degraded = cur_rate < threshold_pct and delta < 0
        return {
            "table": table_name,
            "current_pass_rate": cur_rate,
            "previous_pass_rate": prev_rate,
            "delta_pct": delta,
            "degraded": is_degraded,
            "threshold_pct": threshold_pct,
            "alert_recommendation": "建议触发质量告警" if is_degraded else "质量正常",
            "computed_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg