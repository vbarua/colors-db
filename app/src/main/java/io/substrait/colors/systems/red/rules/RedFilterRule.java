package io.substrait.colors.systems.red.rules;

import io.substrait.colors.systems.red.RedFilter;
import io.substrait.colors.systems.red.RedRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RedFilterRule extends RedConverterRule {

  public static final RedFilterRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              LogicalFilter.class,
              Convention.NONE,
              RedRel.CONVENTION,
              RedFilterRule.class.getName())
          .withRuleFactory(RedFilterRule::new)
          .toRule(RedFilterRule.class);

  protected RedFilterRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    Filter filter = (Filter) rel;
    RelNode input = convert(filter.getInput(), filter.getTraitSet().replace(RedRel.CONVENTION));
    return RedFilter.create(input, filter.getCondition());
  }
}
