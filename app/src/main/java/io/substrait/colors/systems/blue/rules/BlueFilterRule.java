package io.substrait.colors.systems.blue.rules;

import io.substrait.colors.systems.blue.BlueFilter;
import io.substrait.colors.systems.blue.BlueRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BlueFilterRule extends BlueConverterRule {

  public static final BlueFilterRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              LogicalFilter.class,
              Convention.NONE,
              BlueRel.CONVENTION,
              BlueFilterRule.class.getName())
          .withRuleFactory(BlueFilterRule::new)
          .toRule(BlueFilterRule.class);

  protected BlueFilterRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    return BlueFilter.validFilter(filter);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    Filter filter = (Filter) rel;
    RelNode input = convert(filter.getInput(), filter.getTraitSet().replace(BlueRel.CONVENTION));
    return BlueFilter.create(input, filter.getCondition());
  }
}
