"""业务编排API：跨模块的ETL编排、质量门禁、回刷、Schema变更等操作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.engine.alert_matcher import AlertLevel, AlertRule
from app.engine.backfill_plan import BackfillEngine
from app.engine.quality_counting import (
    ExecutionLedgerError,
    NotGateEligibleError,
    QualityCountLedger,
    RuleCountStatus,
    RuleRowCount,
)
from app.engine.validation_executor import CheckLevel, CheckType, QualityRule
from app.engine.watermark_progress import WatermarkEngine, WatermarkRecord, WatermarkStatus
from app.orchestrator import ETLOrchestrator, QualityGateInput

router = APIRouter(prefix="/api/business", tags=["业务编排"])

_orchestrator = ETLOrchestrator()
_ledger = QualityCountLedger()


# ---- 管线编排 ----

@router.post("/pipelines/orchestrate")
def orchestrate_pipeline(body: dict[str, Any]) -> dict[str, Any]:
    """为管线执行完整的DAG拓扑验证和血缘登记。

    body:
      pipeline_id, pipeline_name, source_table, target_table,
      stages: [{"name": "extract", "nodes": [{"id": "n1", "name": "读取"}]}, ...]
    """
    try:
        return _orchestrator.create_pipeline_with_full_orchestration(
            pipeline_id=body["pipeline_id"],
            pipeline_name=body["pipeline_name"],
            source_table=body["source_table"],
            target_table=body["target_table"],
            stages=body.get("stages", []),
        )
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {e}")


# ---- 质量门禁 ----

@router.post("/quality/gate")
def run_quality_gate(body: dict[str, Any]) -> dict[str, Any]:
    """对管线数据执行质量门禁。

    body:
      pipeline_id, rules: [{"rule_id", "rule_name", "target_table", "check_field",
                             "check_type", "threshold", "level", "extra": {}}],
      sample_data: [dict]
    """
    pipeline_id = body["pipeline_id"]
    rules = [
        QualityRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            target_table=r.get("target_table", ""),
            check_field=r["check_field"],
            check_type=CheckType(r["check_type"]),
            threshold=float(r.get("threshold", 0.01)),
            level=CheckLevel(r.get("level", "WARN")),
            extra=r.get("extra", {}),
        )
        for r in body.get("rules", [])
    ]
    result = _orchestrator.run_quality_gate(
        pipeline_id, rules, body.get("sample_data", []),
    )
    return {
        "pipeline_id": result.pipeline_id,
        "overall_pass": result.overall_pass,
        "decision": result.decision,
        "failed_ratio": result.failed_ratio,
        "results": result.results,
        "counting": result.counting,
        "triggered_alerts": [
            {"rule_id": a.rule_id, "rule_name": a.rule_name,
             "matched": a.matched, "level": a.level.value, "message": a.message}
            for a in result.triggered_alerts
        ],
    }


# ---- 计数归档（完整执行凭据） ----

@router.get("/quality/runs")
def list_quality_runs() -> dict[str, Any]:
    """列出计数账本中的执行、归档记录与归档表分母。"""
    return {
        "archives": [record.to_dict() for record in _ledger.archive_records()],
        "archive_execution_count": _ledger.archived_execution_count(),
        "archive_execution_ids": list(_ledger.archived_execution_ids()),
    }


@router.get("/quality/runs/{execution_id}/boundary")
def quality_run_boundary(execution_id: str) -> dict[str, Any]:
    """检查诊断计数能否交接给门禁，以及批次何时允许计入归档。"""
    try:
        return _ledger.handoff_decision(execution_id)
    except ExecutionLedgerError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/quality/runs")
def begin_quality_run(body: dict[str, Any]) -> dict[str, Any]:
    """登记一次执行。

    body: execution_id, pipeline_id, rule_ids, attempt, retry_of
    重试必须登记新的执行编号，否则会把两条规则的样本覆盖当成两次执行。
    """
    try:
        state = _ledger.begin(
            body["execution_id"],
            body["pipeline_id"],
            body.get("rule_ids", []),
            attempt=int(body.get("attempt", 1)),
            retry_of=body.get("retry_of"),
        )
    except ExecutionLedgerError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return state.to_dict()


@router.post("/quality/runs/{execution_id}/rules")
def record_quality_rule(execution_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """登记一条规则的终态诊断。

    body: rule_id, status (COMPLETED/FAILED), 以及完成时的
          total_rows/passed_rows/failed_rows，或失败时的 error。
    """
    try:
        if RuleCountStatus(body.get("status", "COMPLETED")) == RuleCountStatus.FAILED:
            count = _ledger.record_rule_failure(
                execution_id, body["rule_id"], body.get("error", "规则执行失败"),
            )
        elif "result" in body:
            count = _ledger.record_result(execution_id, _ResultShim(body["result"]))
        else:
            count = _ledger.record_rule_count(execution_id, RuleRowCount(
                execution_id=execution_id,
                rule_id=body["rule_id"],
                total_rows=int(body.get("total_rows", 0)),
                passed_rows=int(body.get("passed_rows", 0)),
                failed_rows=int(body.get("failed_rows", 0)),
                status=RuleCountStatus.COMPLETED,
            ))
    except ExecutionLedgerError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"缺少字段: {exc}")
    return count.to_dict()


class _ResultShim:
    """把请求体里的规则结果转换成计数模块需要的只读形状。"""

    def __init__(self, payload: dict[str, Any]) -> None:
        self.rule_id = payload["rule_id"]
        self.total_rows = int(payload.get("total_rows", payload.get("total", 0)))
        self.passed_rows = int(payload.get("passed_rows", payload.get("passed", 0)))
        self.failed_rows = int(payload.get("failed_rows", payload.get("failed", 0)))
        total = self.total_rows or 1
        self.failed_ratio = payload.get("failed_ratio", self.failed_rows / total)


@router.post("/quality/runs/{execution_id}/archive")
def archive_quality_run(execution_id: str) -> dict[str, Any]:
    """收口一次完整执行并写归档记录；部分完成或含失败规则的执行会被拒绝。"""
    try:
        state = _ledger.finalize(execution_id)
    except ExecutionLedgerError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return state.to_dict()


# ---- 门禁消费可核对计数 ----

@router.post("/quality/gate/from-counts")
def run_quality_gate_from_counts(body: dict[str, Any]) -> dict[str, Any]:
    """用已归档的完整执行计数运行门禁（不重新执行校验）。

    body: pipeline_id, execution_id,
          rules: [{"rule_id", "rule_name", "target_table", "check_field",
                   "check_type", "threshold", "level", "extra": {}}]
    """
    pipeline_id = body["pipeline_id"]
    execution_id = body["execution_id"]
    rules = [
        QualityRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            target_table=r.get("target_table", ""),
            check_field=r["check_field"],
            check_type=CheckType(r["check_type"]),
            threshold=float(r.get("threshold", 0.01)),
            level=CheckLevel(r.get("level", "WARN")),
            extra=r.get("extra", {}),
        )
        for r in body.get("rules", [])
    ]
    try:
        state = _ledger.get(execution_id)
        counting = _ledger.handoff_to_gate(execution_id)
    except NotGateEligibleError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ExecutionLedgerError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    result = _orchestrator.run_quality_gate_from_counts(
        pipeline_id,
        rules,
        QualityGateInput(
            pipeline_id=state.pipeline_id,
            execution_id=execution_id,
            counting=counting,
            rule_counts=list(state.ordered_counts),
            confirmation_times={
                count.rule_id: count.confirmed_at or ""
                for count in state.ordered_counts
            },
        ),
    )
    return {
        "pipeline_id": result.pipeline_id,
        "execution_id": execution_id,
        "overall_pass": result.overall_pass,
        "decision": result.decision,
        "failed_ratio": result.failed_ratio,
        "results": result.results,
        "counting": result.counting,
        "triggered_alerts": [
            {"rule_id": a.rule_id, "rule_name": a.rule_name,
             "matched": a.matched, "level": a.level.value, "message": a.message}
            for a in result.triggered_alerts
        ],
    }


# ---- 水位与回刷 ----

@router.post("/watermark/check")
def check_watermark(body: dict[str, Any]) -> dict[str, Any]:
    """检查水位并判断是否需要回刷。

    body:
      pipeline_id, partition_field, current_watermark, new_watermark,
      scheduled_time (ISO), backfill_threshold (可选)
    """
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=body["pipeline_id"],
        partition_field=body["partition_field"],
        current_watermark=body["current_watermark"],
        scheduled_time=datetime.fromisoformat(body["scheduled_time"]),
        status=WatermarkStatus(body.get("status", "正常")),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    return _orchestrator.check_watermark_and_backfill(
        record,
        body["new_watermark"],
        backfill_threshold=body.get("backfill_threshold", 3),
    )


# ---- 回刷计划 ----

@router.post("/backfill/plan")
def generate_backfill_plan(body: dict[str, Any]) -> dict[str, Any]:
    """生成回刷补偿方案。

    body:
      pipeline_id, start_partition, end_partition, parallelism, reason
    """
    engine = BackfillEngine()
    plan = engine.plan(
        pipeline_id=body["pipeline_id"],
        start_partition=body["start_partition"],
        end_partition=body["end_partition"],
        parallelism=int(body.get("parallelism", 4)),
        reason=body.get("reason", ""),
        partition_format=body.get("partition_format", "%Y-%m-%d"),
    )
    # 分配资源
    alloc = _orchestrator.allocate_for_pipeline(
        body["pipeline_id"],
        node_count=plan.total_partitions,
        estimated_memory_gb=plan.total_partitions * 0.5,
    )

    return {
        "plan": {
            "pipeline_id": plan.pipeline_id,
            "task_name": plan.task_name,
            "reason": plan.reason,
            "total_partitions": plan.total_partitions,
            "batch_count": len(plan.batches),
            "parallelism": plan.parallelism,
            "estimated_completion": plan.estimated_completion,
            "batches": [
                {
                    "batch_id": b.batch_id,
                    "partitions": b.range.partitions,
                    "count": b.range.count,
                }
                for b in plan.batches
            ],
        },
        "resource_allocation": alloc["allocation"],
    }


# ---- Schema变更评估 ----

@router.post("/schema/evaluate")
def evaluate_schema_change(body: dict[str, Any]) -> dict[str, Any]:
    """评估Schema变更的兼容性和影响。

    body:
      change_id, table_name,
      before_columns: [{"name", "data_type", "nullable", "is_partition"}],
      after_columns: [{"name", "data_type", "nullable", "is_partition"}],
      pipeline_deps: {table_name: [pipeline_ids]}
    """
    from app.engine.schema_evolution import SchemaColumn, SchemaDefinition

    before = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("before_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("before_columns", []) if c.get("is_partition")
        ],
    )
    after = SchemaDefinition(
        table_name=body["table_name"],
        columns=[
            SchemaColumn(
                name=c["name"], data_type=c.get("data_type", "string"),
                nullable=c.get("nullable", True),
                is_partition=c.get("is_partition", False),
            )
            for c in body.get("after_columns", [])
        ],
        partition_columns=[
            c["name"] for c in body.get("after_columns", []) if c.get("is_partition")
        ],
    )
    result = _orchestrator.evaluate_schema_change(
        change_id=body.get("change_id", "auto"),
        before=before,
        after=after,
        pipeline_deps=body.get("pipeline_deps", {}),
    )
    return {
        "change_id": result.change.change_id,
        "table": result.change.target_table,
        "compatibility": result.change.compatibility.value,
        "changes": result.change.detail.get("changes", []),
        "affected_pipelines": result.change.affected_pipelines,
        "rollback_plan": result.change.rollback_plan,
        "migration_recommendation": result.migration_recommendation,
        "walk_pipelines": result.affected_pipelines,
    }


# ---- 血缘影响分析 ----

@router.get("/lineage/impact/{node_id}")
def analyze_lineage_impact(node_id: str) -> dict[str, Any]:
    """分析数据血缘影响：上游溯源 + 下游影响。"""
    result = _orchestrator.lineage.analyze_impact(node_id)
    return {
        "target_node": result.target_node,
        "upstream_count": len(result.upstream_nodes),
        "downstream_count": len(result.downstream_nodes),
        "affected_pipelines": result.affected_pipelines,
        "lineage_depth": result.lineage_depth,
        "has_cycle": result.has_cycle,
        "cycle_path": result.cycle_path,
    }


# ---- 资源健康 ----

@router.get("/resources/health")
def cluster_health() -> dict[str, Any]:
    """计算资源集群健康状态。"""
    return _orchestrator.balancer.cluster_health()


# ---- 告警批量评估 ----

@router.post("/alerts/evaluate")
def evaluate_alerts(body: dict[str, Any]) -> dict[str, Any]:
    """批量评估告警规则。

    body:
      rules: [{"rule_id", "rule_name", "monitor_target", "condition", "level"}],
      metrics: dict
    """
    rules = [
        AlertRule(
            rule_id=r["rule_id"],
            rule_name=r["rule_name"],
            monitor_target=r["monitor_target"],
            condition=r["condition"],
            level=AlertLevel(r.get("level", "WARN")),
            channels=r.get("channels", ["email"]),
        )
        for r in body.get("rules", [])
    ]
    matcher = _orchestrator.alert
    results = matcher.match_batch(rules, body.get("metrics", {}))

    return {
        "total_rules": len(rules),
        "triggered": sum(1 for r in results if r.matched),
        "results": [
            {
                "rule_id": r.rule_id,
                "rule_name": r.rule_name,
                "matched": r.matched,
                "level": r.level.value,
                "message": r.message,
            }
            for r in results
        ],
    }


# ---- DAG拓扑分析 ----

@router.post("/dag/analyze")
def analyze_dag(body: dict[str, Any]) -> dict[str, Any]:
    """分析DAG拓扑结构。

    body:
      nodes: [{"id", "name", "weight"}],
      edges: [{"from", "to"}]
    """
    from app.engine.dag_topology import DagNode, DagTopology

    topo = DagTopology()
    for n in body.get("nodes", []):
        topo.add_node(DagNode(
            node_id=n["id"], name=n.get("name", n["id"]),
            weight=float(n.get("weight", 1.0)),
        ))
    for e in body.get("edges", []):
        topo.add_edge(e["from"], e["to"])

    result = topo.solve()
    return {
        "node_count": topo.node_count,
        "has_cycle": result.has_cycle,
        "cycle_nodes": result.cycle_nodes,
        "topology_order": result.order,
        "critical_path": result.critical_path,
        "critical_length": result.critical_length,
        "node_depth": result.node_depth,
    }


# ---- 综合管线运行报告 ----

@router.get("/pipelines/{pipeline_id}/run-report")
def pipeline_run_report(pipeline_id: str) -> dict[str, Any]:
    """获取管线的综合运行报告。"""
    # 模拟生成运行报告
    from datetime import datetime

    record = WatermarkRecord(
        pipeline_id=pipeline_id,
        partition_field="dt",
        current_watermark=datetime.now().strftime("%Y-%m-%d"),
        scheduled_time=datetime.now(),
        delay_minutes=0,
        status=WatermarkStatus.NORMAL,
    )

    # 模拟质量结果
    quality = _orchestrator.run_quality_gate(
        pipeline_id,
        [
            QualityRule(
                rule_id="r_default",
                rule_name="默认非空检查",
                target_table=f"dw.{pipeline_id}",
                check_field="id",
                check_type=CheckType.NOT_NULL,
                threshold=0.01,
            ),
        ],
        [{"id": "001"}, {"id": None}],
    )

    return _orchestrator.pipeline_run_report(pipeline_id, record, quality)