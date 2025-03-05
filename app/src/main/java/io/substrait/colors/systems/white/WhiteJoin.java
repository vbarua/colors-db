package io.substrait.colors.systems.white;

import java.util.Collections;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WhiteJoin extends Join implements WhiteRel {

  private WhiteJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType) {
    super(
        cluster,
        traitSet,
        Collections.emptyList(),
        left,
        right,
        condition,
        Collections.emptySet(),
        joinType);
    assert getConvention() == CONVENTION;
  }

  public static WhiteJoin create(
      RelNode left, RelNode right, RexNode condition, JoinRelType joinType) {
    RelOptCluster cluster = left.getCluster();
    RelTraitSet traitSet =
        cluster
            .traitSet()
            .replace(CONVENTION)
            // cannot preserve sort
            .replace(RelCollations.EMPTY);
    return new WhiteJoin(cluster, traitSet, left, right, condition, joinType);
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new WhiteJoin(getCluster(), traitSet, left, right, conditionExpr, joinType);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(COST_SCALING);
  }
}
