package io.substrait.colors.systems.white.rules;

import io.substrait.colors.systems.white.WhiteFilter;
import io.substrait.colors.systems.white.WhiteRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WhiteFilterRule extends WhiteConverterRule {

  public static final WhiteFilterRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              LogicalFilter.class,
              Convention.NONE,
              WhiteRel.CONVENTION,
              WhiteFilterRule.class.getName())
          .withRuleFactory(WhiteFilterRule::new)
          .toRule(WhiteFilterRule.class);

  protected WhiteFilterRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    Filter filter = (Filter) rel;
    RelNode input = convert(filter.getInput(), filter.getTraitSet().replace(WhiteRel.CONVENTION));
    return WhiteFilter.create(input, filter.getCondition());
  }
}
