"""ETL编排中心 — 跨模块业务编排层。

将各引擎组合成完整业务链路：
- 管线创建 → DAG拓扑验证 → 节点登记 → 质量规则绑定 → 调度激活
- Schema变更 → 兼容性评估 → 影响管线遍历 → 配置迁移 → 回滚方案
- 水位延迟 → 回刷补偿 → 分区拆分 → 资源分配 → 进度追踪
- 质量校验 → 异常捕获 → 告警匹配 → 阻断/通过决策
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.engine.alert_matcher import AlertLevel, AlertMatcher, AlertResult, AlertRule
from app.engine.backfill_plan import BackfillEngine, BackfillPlan
from app.engine.dag_topology import DagNode, DagTopology, TopologyResult
from app.engine.lineage_graph import (
    ImpactAnalysis, LineageEdge, LineageGraphEngine, LineageNode,
)
from app.engine.resource_load_balancer import (
    AllocationResult, ResourceLoadBalancer, ResourcePool, TaskResource,
)
from app.engine.schema_evolution import (
    Compatibility, SchemaChange, SchemaColumn, SchemaDefinition,
    SchemaEvolutionEngine,
)
from app.engine.validation_executor import (
    CheckLevel, CheckType, QualityRule, ValidationEngine, ValidationResult,
)
from app.engine.quality_counting import GateCounting, RuleRowCount
from app.engine.watermark_progress import (
    WatermarkAdvanceResult, WatermarkEngine, WatermarkRecord, WatermarkStatus,
)


@dataclass
class PipelineCreateContext:
    """创建ETL管线的完整上下文。"""
    pipeline_id: str
    pipeline_name: str
    source_table: str
    target_table: str
    stages: list[str]
    nodes: list[DagNode] = field(default_factory=list)
    quality_rules: list[QualityRule] = field(default_factory=list)
    topology_result: TopologyResult | None = None


@dataclass
class QualityGateResponse:
    """质量门禁结果。

    ``failed_ratio`` 与 ``results`` 沿用原口径；``counting`` 是可核对的行计数，
    记录本次门禁纳入了哪些规则、分母由哪些规则构成。
    """
    pipeline_id: str
    overall_pass: bool
    decision: str                     # RELEASE / BLOCKED
    failed_ratio: float
    results: list[dict[str, Any]]
    triggered_alerts: list[AlertResult]
    counting: GateCounting | None = None


@dataclass
class QualityGateInput:
    """可交给门禁的计数依据：只接受完整执行的计数。"""
    pipeline_id: str
    execution_id: str
    counting: GateCounting
    rule_counts: list[RuleRowCount]
    confirmation_times: dict[str, str]


@dataclass
class BackfillOrchestrateResult:
    """回刷编排结果。"""
    pipeline_id: str
    plan: BackfillPlan
    allocation: AllocationResult | None = None
    quality_gate: QualityGateResponse | None = None


@dataclass
class SchemaChangeOrchestrateResult:
    """Schema变更编排结果。"""
    change: SchemaChange
    affected_pipelines: list[str]
    migration_recommendation: str
    impact_summary: dict[str, Any]


class ETLOrchestrator:
    """ETL编排总控制器。

    组合所有引擎，提供完整业务链路操作入口。
    """

    def __init__(self) -> None:
        self.dag = DagTopology()
        self.validation = ValidationEngine()
        self.watermark = WatermarkEngine()
        self.backfill = BackfillEngine()
        self.lineage = LineageGraphEngine()
        self.balancer = ResourceLoadBalancer()
        self.schema_evolution = SchemaEvolutionEngine()
        self.alert = AlertMatcher()

        # 注册默认资源池
        self.balancer.register_pool(ResourcePool(
            pool_id="pool_prod", pool_name="生产资源池",
            total_cores=128, total_memory_gb=512, max_concurrency=50,
        ))
        self.balancer.register_pool(ResourcePool(
            pool_id="pool_test", pool_name="测试资源池",
            total_cores=32, total_memory_gb=128, max_concurrency=20,
        ))
        self.balancer.register_pool(ResourcePool(
            pool_id="pool_adhoc", pool_name="临时计算池",
            total_cores=16, total_memory_gb=64, max_concurrency=10,
        ))

    # ---- 管线编排 ----

    def create_pipeline(
        self, ctx: PipelineCreateContext
    ) -> dict[str, Any]:
        """创建ETL管线：建节点 → DAG验证 → 质量规则绑定。"""
        for node in ctx.nodes:
            self.dag.add_node(node)

        # 按阶段连接节点（每个阶段之间全连接）
        stage_nodes: dict[str, list[str]] = {}
        for node in ctx.nodes:
            stage = node.meta.get("stage", "")
            stage_nodes.setdefault(stage, []).append(node.node_id)

        stage_order = sorted(stage_nodes.keys())
        for i in range(len(stage_order) - 1):
            for from_id in stage_nodes[stage_order[i]]:
                for to_id in stage_nodes[stage_order[i + 1]]:
                    self.dag.add_edge(from_id, to_id)

        # 拓扑验证
        topology = self.dag.solve()
        ctx.topology_result = topology

        return {
            "pipeline_id": ctx.pipeline_id,
            "pipeline_name": ctx.pipeline_name,
            "source_table": ctx.source_table,
            "target_table": ctx.target_table,
            "node_count": len(ctx.nodes),
            "stage_count": len(stage_order),
            "has_cycle": topology.has_cycle,
            "topology_order": topology.order,
            "critical_path": topology.critical_path,
            "max_depth": max(topology.node_depth.values()) if topology.node_depth else 0,
            "created_at": datetime.now().isoformat(),
        }

    def create_pipeline_with_full_orchestration(
        self,
        pipeline_id: str,
        pipeline_name: str,
        source_table: str,
        target_table: str,
        stages: list[dict[str, Any]],
        quality_rules: list[QualityRule] | None = None,
    ) -> dict[str, Any]:
        """完整管线编排：DAG + 质量 + 调度 + 血缘。"""
        # 建节点
        nodes: list[DagNode] = []
        for stage in stages:
            for node_def in stage.get("nodes", []):
                nodes.append(DagNode(
                    node_id=node_def["id"],
                    name=node_def["name"],
                    weight=node_def.get("weight", 1.0),
                    meta={"stage": stage["name"], **node_def.get("meta", {})},
                ))

        ctx = PipelineCreateContext(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            source_table=source_table,
            target_table=target_table,
            stages=[s["name"] for s in stages],
            nodes=nodes,
            quality_rules=quality_rules or [],
        )
        result = self.create_pipeline(ctx)

        # 登记血缘
        self.lineage.add_node(LineageNode(source_table, source_table, node_type="source"))
        self.lineage.add_node(LineageNode(target_table, target_table, node_type="target"))
        self.lineage.add_edge(LineageEdge(
            edge_id=f"edge_{source_table}_to_{target_table}",
            source=source_table,
            target=target_table,
            transfer_fields=[n.name for n in nodes if n.meta.get("stage") == stages[0]["name"]],
            transform_type="etl_pipeline",
        ))

        result["lineage_edge"] = f"edge_{source_table}_to_{target_table}"
        result["quality_rules_count"] = len(ctx.quality_rules)

        return result

    # ---- 质量门禁 ----

    def run_quality_gate(
        self,
        pipeline_id: str,
        rules: list[QualityRule],
        sample_data: list[dict[str, Any]],
    ) -> QualityGateResponse:
        """执行管线质量门禁。"""
        result = self.validation.run_pipeline_quality(
            rules, sample_data, pipeline_id,
            execution_id=f"{pipeline_id}#gate",
        )
        return self._build_gate_response(
            pipeline_id=pipeline_id,
            rules=rules,
            overall_pass=result["overall_pass"],
            decision=result["decision"],
            failed_ratio=result["overall_failed_ratio"],
            results=result["rule_results"],
            counting=result["counting"],
        )

    def run_quality_gate_from_counts(
        self,
        pipeline_id: str,
        rules: list[QualityRule],
        gate_input: QualityGateInput,
    ) -> QualityGateResponse:
        """消费已确认的逐规则计数运行门禁（不重新执行校验）。

        只接受完整执行的计数；部分完成或含失败规则的执行会被拒绝，
        需要改走 ``run_quality_gate`` 或换新执行编号重试。
        """
        if gate_input.pipeline_id != pipeline_id:
            raise ValueError(
                f"计数归属的管线 {gate_input.pipeline_id} 与门禁管线 {pipeline_id} 不一致"
            )
        counts_by_rule = {count.rule_id: count for count in gate_input.rule_counts}
        missing = [rule.rule_id for rule in rules if rule.rule_id not in counts_by_rule]
        if missing:
            raise ValueError(
                "门禁规则缺少已确认的计数：" + "、".join(missing)
            )
        failed_ratio = gate_input.counting.failed_ratio
        rule_results = []
        for rule in rules:
            count = counts_by_rule[rule.rule_id]
            # 阻断判定沿用原口径：坏行比例对各规则自己的分母
            blocked = (
                rule.level == CheckLevel.BLOCK
                and count.failed_ratio > rule.threshold
            )
            rule_results.append({
                "rule_id": rule.rule_id,
                "total": count.total_rows,
                "passed": count.passed_rows,
                "failed": count.failed_rows,
                "ratio": round(count.failed_ratio, 4),
                "blocked": blocked,
            })
        any_blocked = any(item["blocked"] for item in rule_results)
        return self._build_gate_response(
            pipeline_id=pipeline_id,
            rules=rules,
            overall_pass=not any_blocked,
            decision="RELEASE" if not any_blocked else "BLOCKED",
            failed_ratio=failed_ratio,
            results=rule_results,
            counting=gate_input.counting.to_dict(),
        )

    def _build_gate_response(
        self,
        *,
        pipeline_id: str,
        rules: list[QualityRule],
        overall_pass: bool,
        decision: str,
        failed_ratio: float,
        results: list[dict[str, Any]],
        counting: dict[str, Any] | None,
    ) -> QualityGateResponse:
        """告警评估与响应装配；告警仍按门禁的百分比口径判断。"""
        alert_rules = [
            AlertRule(
                rule_id=f"qalert_{r.rule_id}",
                rule_name=f"质量检查:{r.rule_name}",
                monitor_target=pipeline_id,
                condition=f"failed_ratio > {r.threshold}",
                level=AlertLevel.WARN if r.level == CheckLevel.WARN else AlertLevel.CRITICAL,
            )
            for r in rules
        ]
        alerts = self.alert.match_batch(alert_rules, {"failed_ratio": failed_ratio})

        return QualityGateResponse(
            pipeline_id=pipeline_id,
            overall_pass=overall_pass,
            decision=decision,
            failed_ratio=failed_ratio,
            results=results,
            triggered_alerts=alerts,
            counting=counting,
        )

    # ---- 水位与回刷 ----

    def check_watermark_and_backfill(
        self,
        record: WatermarkRecord,
        new_watermark: str,
        backfill_threshold: int = 3,
    ) -> dict[str, Any]:
        """检查水位并决定是否触发回刷。"""
        advance = self.watermark.advance(record, new_watermark)

        need_backfill, backfill_reason = self.watermark.should_trigger_backfill(
            record, backfill_threshold,
        )

        result: dict[str, Any] = {
            "pipeline_id": record.pipeline_id,
            "old_watermark": advance.old_watermark,
            "new_watermark": advance.new_watermark,
            "delay_minutes": advance.delay_minutes,
            "new_status": advance.new_status.value,
            "need_backfill": need_backfill,
            "backfill_reason": backfill_reason or "",
        }

        if need_backfill:
            try:
                from datetime import datetime, timedelta
                wm_dt = datetime.strptime(record.current_watermark, record.partition_format)
                expected = datetime.now() - timedelta(days=1)
                plan = self.backfill.plan(
                    pipeline_id=record.pipeline_id,
                    start_partition=record.current_watermark,
                    end_partition=expected.strftime(record.partition_format),
                    parallelism=4,
                    reason=backfill_reason or "水位延迟自动触发",
                )
                result["backfill_plan"] = {
                    "total_partitions": plan.total_partitions,
                    "batch_count": len(plan.batches),
                    "parallelism": plan.parallelism,
                    "estimated_completion": plan.estimated_completion,
                }
            except ValueError:
                result["backfill_plan"] = {"error": "无法生成回刷方案"}

        return result

    # ---- Schema变更 ----

    def evaluate_schema_change(
        self,
        change_id: str,
        before: SchemaDefinition,
        after: SchemaDefinition,
        pipeline_deps: dict[str, list[str]],
    ) -> SchemaChangeOrchestrateResult:
        """评估Schema变更并生成完整影响报告。"""
        change = self.schema_evolution.evaluate_change(
            change_id, before, after, pipeline_deps,
        )

        # 遍历受影响的管线
        affected = self.schema_evolution.walk_affected_pipelines(
            before.table_name, pipeline_deps,
        )

        migration_rec = ""
        if change.compatibility == Compatibility.INCOMPATIBLE:
            migration_rec = (
                f"不兼容变更，请联系 {len(change.affected_pipelines)} 条受影响管线的负责人，"
                f"先调整下游ETL逻辑再执行Schema变更。回滚方案：{change.rollback_plan[:200]}..."
            )
        elif change.compatibility == Compatibility.COMPATIBLE:
            migration_rec = "兼容变更，可直接部署，无需修改下游管线。"
        else:
            migration_rec = "待评估，请确认变更详情后再决定。"

        return SchemaChangeOrchestrateResult(
            change=change,
            affected_pipelines=[item["table"] for item in affected],
            migration_recommendation=migration_rec,
            impact_summary={
                "change_id": change_id,
                "table": before.table_name,
                "compatibility": change.compatibility.value,
                "total_changes": len(change.detail.get("changes", [])),
                "affected_pipeline_count": len(change.affected_pipelines),
                "rollback_available": bool(change.rollback_plan),
            },
        )

    # ---- 资源分配 ----

    def allocate_for_pipeline(
        self, pipeline_id: str, node_count: int, estimated_memory_gb: float
    ) -> dict[str, Any]:
        """为管线分配计算资源。"""
        task = TaskResource(
            task_id=f"task_{pipeline_id}",
            required_cores=min(node_count * 2, 32),
            required_memory_gb=estimated_memory_gb,
            priority=1,
            labels={"pipeline": pipeline_id},
        )
        result = self.balancer.allocate(task)

        health = self.balancer.cluster_health()
        return {
            "pipeline_id": pipeline_id,
            "allocation": {
                "allocated": result.allocated,
                "pool_id": result.pool_id,
                "pool_name": result.pool_name,
                "reason": result.reason,
            },
            "cluster_health": health,
            "required_cores": task.required_cores,
            "required_memory_gb": task.required_memory_gb,
        }

    # ---- 综合运行报告 ----

    def pipeline_run_report(
        self,
        pipeline_id: str,
        watermark: WatermarkRecord,
        quality_result: QualityGateResponse | None = None,
    ) -> dict[str, Any]:
        """管线运行综合报告：水位 + 质量 + 告警。"""
        report: dict[str, Any] = {
            "pipeline_id": pipeline_id,
            "timestamp": datetime.now().isoformat(),
        }

        # 水位
        report["watermark"] = {
            "current": watermark.current_watermark,
            "delay_minutes": watermark.delay_minutes,
            "status": watermark.status.value,
        }

        # 质量
        if quality_result:
            report["quality"] = {
                "pass": quality_result.overall_pass,
                "decision": quality_result.decision,
                "failed_ratio": quality_result.failed_ratio,
            }
            report["alerts"] = [
                {"name": a.rule_name, "level": a.level.value, "message": a.message}
                for a in quality_result.triggered_alerts if a.matched
            ]

        # 整体健康评估
        checks = []
        if watermark.status in (WatermarkStatus.DELAYED, WatermarkStatus.SEVERE):
            checks.append("水位延迟")
        if quality_result and not quality_result.overall_pass:
            checks.append("质量门禁不通过")
        if report.get("alerts"):
            checks.append("有未处理告警")

        report["health"] = "healthy" if not checks else "degraded" if len(checks) <= 2 else "critical"
        report["issues"] = checks

        return report