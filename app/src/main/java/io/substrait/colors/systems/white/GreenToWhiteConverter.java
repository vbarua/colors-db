package io.substrait.colors.systems.white;

import io.substrait.colors.systems.green.GreenRel;
import io.substrait.colors.systems.white.rules.GreenToWhiteConverterRule;
import java.util.List;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

/**
 * {@link RelNode} which represents the movement of data from GREEN to WHITE
 *
 * <p>See: {@link GreenToWhiteConverterRule}
 */
public class GreenToWhiteConverter extends ConverterImpl implements WhiteRel {

  private GreenToWhiteConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    assert input.getTraitSet().getConvention() == GreenRel.CONVENTION;
    assert traits.getConvention() == WhiteRel.CONVENTION;
  }

  public static GreenToWhiteConverter create(RelNode input) {
    RelTraitSet newTraitSet = input.getTraitSet().replace(CONVENTION);
    return new GreenToWhiteConverter(input.getCluster(), newTraitSet, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new GreenToWhiteConverter(getCluster(), traitSet, sole(inputs));
  }
}
