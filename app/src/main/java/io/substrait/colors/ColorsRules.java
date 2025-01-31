package io.substrait.colors;

import io.substrait.colors.systems.white.rules.RedToWhiteConverterRule;
import io.substrait.colors.systems.white.rules.WhiteProjectRule;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;

/**
 * Organizes the various {@link RelOptRule}s used when optimizing queries for the RED, GREEN, BLUE
 * and WHITE
 */
public class ColorsRules {

  /** Optimization rules associated with WHITE */
  public static final List<RelOptRule> WHITE_RULES =
      // Allows the planner to insert a relation that converts from RED to WHITE
      List.of(RedToWhiteConverterRule.INSTANCE, WhiteProjectRule.INSTANCE);
}
