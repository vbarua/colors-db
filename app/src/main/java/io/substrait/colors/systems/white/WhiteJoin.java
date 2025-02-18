package io.substrait.colors.systems.white;

import java.util.Collections;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

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
    assert left.getConvention() == CONVENTION;
    assert right.getConvention() == CONVENTION;
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
    return new WhiteJoin(left.getCluster(), traitSet, left, right, conditionExpr, joinType);
  }
}
