package io.substrait.colors.systems.green.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Base class for planner rules that convert a relational expression to the GREEN calling convention
 */
abstract class GreenConverterRule extends ConverterRule {

  @Override
  public Convention getOutConvention() {
    return super.getOutConvention();
  }

  protected GreenConverterRule(Config config) {
    super(config);
  }
}
