package io.substrait.colors.systems.white;

import io.substrait.colors.systems.blue.BlueRel;
import io.substrait.colors.systems.white.rules.BlueToWhiteConverterRule;
import java.util.List;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

/**
 * {@link RelNode} which represents the movement of data from BLUE to WHITE
 *
 * <p>See: {@link BlueToWhiteConverterRule}
 */
public class BlueToWhiteConverter extends ConverterImpl implements WhiteRel {

  private BlueToWhiteConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    assert input.getTraitSet().getConvention() == BlueRel.CONVENTION;
    assert traits.getConvention() == WhiteRel.CONVENTION;
  }

  public static BlueToWhiteConverter create(RelNode input) {
    RelTraitSet newTraitSet = input.getTraitSet().replace(CONVENTION);
    return new BlueToWhiteConverter(input.getCluster(), newTraitSet, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BlueToWhiteConverter(getCluster(), traitSet, sole(inputs));
  }
}
