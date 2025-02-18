package io.substrait.colors.systems.blue.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Base class for planner rules that convert a relational expression to the BLUE calling convention
 */
abstract class BlueConverterRule extends ConverterRule {

  @Override
  public Convention getOutConvention() {
    return super.getOutConvention();
  }

  protected BlueConverterRule(Config config) {
    super(config);
  }
}
