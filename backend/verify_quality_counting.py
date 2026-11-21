"""质量计数重构的对照验证脚本。

覆盖任务要求的三个对照数据集（空输入 / 规则之间覆盖不同 / 整批有阻断），
并模拟「同一执行有两条规则，一条先算完、另一条失败」的交接与归档边界。
直接运行：``python verify_quality_counting.py``（无需 pytest）。
"""

from __future__ import annotations

import json
import sys

from app.engine.quality_counting import (
    CONFIRMED_BATCH_STATUSES,
    ExecutionFailedError,
    ExecutionStatus,
    IncompleteExecutionError,
    NotGateEligibleError,
    QualityCountLedger,
    build_table_countings,
    collect_execution_batches,
    row_ratio,
)
from app.engine.validation_executor import (
    CheckLevel,
    CheckType,
    QualityRule,
    ValidationEngine,
)
from app.orchestrator import ETLOrchestrator, QualityGateInput
from app.services.validation_run import ValidationRunService

FAILED = 0


def check(label: str, actual, expected) -> None:
    global FAILED
    if actual != expected:
        FAILED += 1
        print(f"[FAIL] {label}\n  expected={expected!r}\n  actual  ={actual!r}")
    else:
        print(f"[ OK ] {label}")


def rule(rule_id: str, field: str, level=CheckLevel.WARN, threshold=0.1) -> QualityRule:
    return QualityRule(
        rule_id=rule_id, rule_name=rule_id, target_table="t",
        check_field=field, check_type=CheckType.NOT_NULL,
        threshold=threshold, level=level,
    )


def strip(counting: dict) -> dict:
    payload = dict(counting)
    payload.pop("rule_counts", None)
    return payload


# ---------------------------------------------------------------------------
# 1) 门禁百分比与表执行通过率：三种数据集对照原始结果
# ---------------------------------------------------------------------------

def gate_cases() -> None:
    engine = ValidationEngine()
    cases = {
        "empty": ([rule("r1", "a"), rule("r2", "b")], []),
        "coverage": (
            [rule("ra", "a"), rule("rb", "b")],
            [{"a": "1", "b": "1"}, {"a": None, "b": "2"},
             {"a": "3", "b": None}, {"a": "4", "b": None}],
        ),
        "blocked": (
            [rule("ra", "a"), rule("rb", "b", CheckLevel.BLOCK)],
            [{"a": None, "b": None}, {"a": "x", "b": "y"}],
        ),
    }
    # 这些是重构前直接运行 run_pipeline_quality 得到的原值。
    expected = {
        "empty": {
            "overall_failed_ratio": 0.0, "overall_pass": True, "decision": "RELEASE",
            "per_rule": [("r1", 1, 0, 0, 0.0), ("r2", 1, 0, 0, 0.0)],
            "denominator": 2, "included": ["r1", "r2"],
        },
        "coverage": {
            "overall_failed_ratio": 0.375, "overall_pass": True, "decision": "RELEASE",
            "per_rule": [("ra", 4, 3, 1, 0.25), ("rb", 4, 2, 2, 0.5)],
            "denominator": 8, "included": ["ra", "rb"],
        },
        "blocked": {
            "overall_failed_ratio": 0.5, "overall_pass": False, "decision": "BLOCKED",
            "per_rule": [("ra", 2, 1, 1, 0.5), ("rb", 2, 1, 1, 0.5)],
            "denominator": 4, "included": ["ra", "rb"],
        },
    }

    for name, (rules, rows) in cases.items():
        result = engine.run_pipeline_quality(rules, rows, "p", execution_id=f"exec-{name}")
        want = expected[name]
        check(f"{name}: overall_failed_ratio",
              result["overall_failed_ratio"], want["overall_failed_ratio"])
        check(f"{name}: overall_pass", result["overall_pass"], want["overall_pass"])
        check(f"{name}: decision", result["decision"], want["decision"])
        per_rule = [
            (item["rule_id"], item["total"], item["passed"], item["failed"], item["ratio"])
            for item in result["rule_results"]
        ]
        check(f"{name}: per-rule rows/ratios", per_rule, want["per_rule"])
        counting = result["counting"]
        check(f"{name}: gate unit", counting["unit"], "RULE_ROWS")
        check(f"{name}: gate denominator", counting["denominator"], want["denominator"])
        check(f"{name}: included rule ids",
              counting["included_rule_ids"], want["included"])
        check(f"{name}: precision", counting["precision"], 4)

    # 只有空输入且无规则时，汇总比例保持 0.0 而不是 0/1 之外的变化
    empty = engine.run_pipeline_quality([], [], "p")
    check("no-rules: ratio", empty["overall_failed_ratio"], 0.0)
    check("no-rules: decision", empty["decision"], "RELEASE")


# ---------------------------------------------------------------------------
# 2) 规则覆盖不同：两条规则分母各自独立，不合并成一次执行
# ---------------------------------------------------------------------------

def coverage_is_not_execution_count() -> None:
    rows = [{"a": "1"}, {"a": None}, {"a": None}]
    engine = ValidationEngine()
    result = engine.run_pipeline_quality(
        [rule("ra", "a"), rule("ra_shadow", "a")], rows, "p", execution_id="exec-cov",
    )
    counting = result["counting"]
    check("coverage: two rule denominators stay separate",
          [entry["denominator"] for entry in counting["rule_counts"]], [3, 3])
    ledger = QualityCountLedger()
    ledger.begin("exec-cov", "p", ["ra", "ra_shadow"])
    for item in engine.run_batch([rule("ra", "a"), rule("ra_shadow", "a")], rows):
        ledger.record_result("exec-cov", item)
    ledger.finalize("exec-cov")
    check("coverage: one execution, two rules -> execution_count 1",
          ledger.archived_execution_count("p"), 1)
    archive = ledger.archive_records()[0].to_dict()
    check("coverage: archive keeps both rule ids",
          archive["included_rule_ids"], ["ra", "ra_shadow"])
    check("coverage: archive row denominator is sum of rule denominators",
          archive["denominator"], 6)


# ---------------------------------------------------------------------------
# 3) 表执行通过率：分母只算已确认执行次数
# ---------------------------------------------------------------------------

def table_rate_cases() -> None:
    rows = [
        {"批次标识": "VAL-1", "目标表": "t1", "总行数": "100", "通过行数": "90",
         "失败行数": "10", "status": "已通过"},
        # 同一执行编号重复写入（重试覆盖），不得累加成两次执行
        {"批次标识": "VAL-1", "目标表": "t1", "总行数": "100", "通过行数": "95",
         "失败行数": "5", "status": "已通过"},
        {"批次标识": "VAL-2", "目标表": "t1", "总行数": "50", "通过行数": "40",
         "失败行数": "10", "status": "未通过"},
        {"批次标识": "VAL-3", "目标表": "t1", "总行数": "0", "通过行数": "0",
         "失败行数": "0", "status": "校验中"},
    ]
    batches = collect_execution_batches(rows)
    check("table: duplicate execution id collapses", len(batches), 3)
    counting = build_table_countings(batches)["t1"]
    check("table: execution_count excludes duplicate row", counting.execution_count, 3)
    check("table: confirmed execution ids", counting.confirmed_execution_ids,
          ("VAL-1", "VAL-2"))
    check("table: execution pass rate uses confirmed denominator",
          counting.execution_pass_rate_pct, 50.0)
    # 行口径沿用原实现：首条 VAL-1 生效 -> 190 通过 / 150 总行
    check("table: row total keeps original row denominator",
          (counting.total_rows, counting.passed_rows), (150, 130))
    check("table: row pass rate precision", counting.row_pass_rate_pct, 86.7)
    check("table: empty denominator on zero rows", row_ratio(0, 0), 0.0)
    check("table: confirmed statuses",
          sorted(CONFIRMED_BATCH_STATUSES), sorted(["已阻断", "未通过", "已通过"]))

    # 服务层原口径对照：单批次通过率保持 2 位小数
    svc = ValidationRunService()
    single = svc.compute_pass_rate("VAL-20261119-001")
    check("report: single batch pass rate unchanged", single["pass_rate_pct"], 99.81)
    check("report: single batch fail rate unchanged", single["fail_rate_pct"], 0.19)
    check("report: counting unit", single["counting"]["unit"], "EXECUTION_BATCH")


# ---------------------------------------------------------------------------
# 4) 交接边界：一条规则先算完、另一条失败
# ---------------------------------------------------------------------------

def boundary_cases() -> None:
    ledger = QualityCountLedger()
    engine = ValidationEngine()
    rows = [{"a": "x", "b": None}, {"a": "y", "b": None}]
    rules = {"ra": rule("ra", "a"), "rb": rule("rb", "b")}

    ledger.begin("exec-partial", "p_retry", ["ra", "rb"], attempt=1)
    ledger.record_result("exec-partial", engine.run(rules["ra"], rows))

    partial = ledger.handoff_decision("exec-partial")
    check("boundary: partial execution not eligible", partial["eligible"], False)
    check("boundary: partial status", partial["status"], "OPEN")
    check("boundary: pending rule id", partial["pending_rule_ids"], ["rb"])
    check("boundary: partial gate counting is None", partial["counting"], None)
    try:
        ledger.handoff_to_gate("exec-partial")
        check("boundary: partial handoff rejected", False, True)
    except NotGateEligibleError:
        check("boundary: partial handoff rejected", True, True)

    try:
        ledger.finalize("exec-partial")
        check("boundary: partial archive rejected", False, True)
    except IncompleteExecutionError:
        check("boundary: partial archive rejected", True, True)
    check("boundary: archive denominator before completion",
          ledger.archived_execution_count("p_retry"), 0)

    # 第二条规则失败：执行不再完整，也不能写归档
    ledger.record_rule_failure("exec-partial", "rb", "executor aborted")
    failed = ledger.handoff_decision("exec-partial")
    check("boundary: failed execution status", failed["status"], "FAILED")
    check("boundary: failed execution not eligible", failed["eligible"], False)
    check("boundary: failed rule excluded from gate denominator",
          failed["excluded_rule_ids"], ["rb"])
    check("boundary: diagnostic denominator only counts completed rule",
          failed["diagnostic_counting"]["denominator"], 2)
    check("boundary: archive still empty",
          ledger.archived_execution_count("p_retry"), 0)

    # 重试必须使用新的执行编号，且不能把两轮样本覆盖当成两次执行
    try:
        ledger.record_result("exec-partial", engine.run(rules["rb"], rows))
        check("boundary: overwrite inside same execution rejected", False, True)
    except Exception:
        check("boundary: overwrite inside same execution rejected", True, True)

    try:
        ledger.finalize("exec-partial")
        check("boundary: failed run never archived", False, True)
    except ExecutionFailedError as exc:
        check("boundary: failed run never archived", "失败" in str(exc), True)

    ledger.begin("exec-retry", "p_retry", ["ra", "rb"], attempt=2, retry_of="exec-partial")
    for rule_id in ("ra", "rb"):
        ledger.record_result("exec-retry", engine.run(rules[rule_id], rows))
    ledger.finalize("exec-retry")

    check("boundary: failed attempt never archived",
          ledger.archived_execution_ids("p_retry"), ("exec-retry",))
    check("boundary: retry counts as exactly one execution",
          ledger.archived_execution_count("p_retry"), 1)
    retry_archive = ledger.archive_records()[0].to_dict()
    check("boundary: retry archive attempt", retry_archive["attempt"], 2)
    check("boundary: retry archive back-reference",
          retry_archive["retry_of"], "exec-partial")
    check("boundary: retry archive execution_count", retry_archive["execution_count"], 1)
    check("boundary: retry archive keeps both rule ids",
          retry_archive["included_rule_ids"], ["ra", "rb"])
    check("boundary: retry archive denominator",
          retry_archive["denominator"], 4)

    # 归档后状态不可回退
    check("boundary: archived status",
          ledger.get("exec-retry").status, ExecutionStatus.ARCHIVED)


# ---------------------------------------------------------------------------
# 5) 门禁消费可核对计数：告警依据与限制
# ---------------------------------------------------------------------------

def gate_consumes_counts() -> None:
    ledger = QualityCountLedger()
    engine = ValidationEngine()
    rows = [{"a": None}, {"a": "ok"}]
    rules = [rule("ra", "a", CheckLevel.WARN, threshold=0.1)]
    ledger.begin("exec-gate", "p_gate", ["ra"])
    ledger.record_result("exec-gate", engine.run(rules[0], rows))
    ledger.finalize("exec-gate")
    counting = ledger.handoff_to_gate("exec-gate")
    orchestrator = ETLOrchestrator()
    response = orchestrator.run_quality_gate_from_counts(
        "p_gate", rules,
        QualityGateInput(
            pipeline_id="p_gate",
            execution_id="exec-gate",
            counting=counting,
            rule_counts=list(ledger.get("exec-gate").ordered_counts),
            confirmation_times={},
        ),
    )
    check("gate-from-counts: failed ratio", response.failed_ratio, 0.5)
    check("gate-from-counts: counted rule ids",
          response.counting["included_rule_ids"], ["ra"])
    # 告警仍按门禁百分比口径：0.5 > 0.1 触发
    matched = [a.rule_id for a in response.triggered_alerts if a.matched]
    check("gate-from-counts: alert follows gate ratio", matched, ["qalert_ra"])

    # 同一告警依据在「已确认执行数」变化时应能被核对
    archive_payload = ledger.archive_records()[0].to_dict()
    check("gate-from-counts: archive carries signature",
          bool(archive_payload["count_signature"]), True)
    check("gate-from-counts: archive unit", archive_payload["unit"], "EXECUTION_BATCH")


def main() -> int:
    gate_cases()
    coverage_is_not_execution_count()
    table_rate_cases()
    boundary_cases()
    gate_consumes_counts()
    print()
    if FAILED:
        print(f"{FAILED} 项不符")
        return 1
    print("全部对照通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
