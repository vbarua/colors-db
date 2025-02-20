package io.substrait.colors.systems.white;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public class WhiteFilter extends Filter implements WhiteRel {

  private WhiteFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert traits.getConvention() == CONVENTION;
  }

  public static WhiteFilter create(RelNode input, RexNode condition) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().replace(CONVENTION);
    return new WhiteFilter(cluster, traitSet, input, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new WhiteFilter(input.getCluster(), traitSet, input, condition);
  }
}
