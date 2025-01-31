package io.substrait.colors.systems.white.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Base class for planner rules that convert a relational expression to the WHITE calling convention
 */
abstract class WhiteConverterRule extends ConverterRule {

  @Override
  public Convention getOutConvention() {
    return super.getOutConvention();
  }

  protected WhiteConverterRule(Config config) {
    super(config);
  }
}
