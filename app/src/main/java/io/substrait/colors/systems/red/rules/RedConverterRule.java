package io.substrait.colors.systems.red.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Base class for planner rules that convert a relational expression to the RED calling convention
 */
abstract class RedConverterRule extends ConverterRule {

  @Override
  public Convention getOutConvention() {
    return super.getOutConvention();
  }

  protected RedConverterRule(Config config) {
    super(config);
  }
}
