package io.substrait.colors.systems.white;

import io.substrait.colors.systems.red.RedRel;
import java.util.List;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

/**
 * {@link RelNode} which represents the movement of data from RED to WHITE
 *
 * <p>See: {@link io.substrait.colors.systems.white.rules.RedToWhiteConverterRule}
 */
public class RedToWhiteConverter extends ConverterImpl implements WhiteRel {

  private RedToWhiteConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    assert input.getTraitSet().getConvention() == RedRel.CONVENTION;
    assert traits.getConvention() == WhiteRel.CONVENTION;
  }

  public static RedToWhiteConverter create(RelNode input) {
    RelTraitSet newTraitSet = input.getTraitSet().replace(CONVENTION);
    return new RedToWhiteConverter(input.getCluster(), newTraitSet, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RedToWhiteConverter(getCluster(), traitSet, sole(inputs));
  }
}
