package io.substrait.colors.systems.red;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RedProject extends Project implements RedRel {

  private RedProject(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      List<RexNode> projects,
      RelDataType rowType) {
    super(
        cluster, traits, Collections.emptyList(), input, projects, rowType, Collections.emptySet());
  }

  public static RedProject create(RelNode input, List<RexNode> projects, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster
            .traitSet()
            .replace(CONVENTION)
            .replaceIfs(
                RelCollationTraitDef.INSTANCE, () -> RelMdCollation.project(mq, input, projects));
    return new RedProject(cluster, traitSet, input, projects, rowType);
  }

  @Override
  public Project copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new RedProject(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(COST_SCALING);
  }
}
