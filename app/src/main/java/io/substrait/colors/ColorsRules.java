package io.substrait.colors;

import io.substrait.colors.systems.red.rules.RedJoinRule;
import io.substrait.colors.systems.white.rules.BlueToWhiteConverterRule;
import io.substrait.colors.systems.white.rules.GreenToWhiteConverterRule;
import io.substrait.colors.systems.white.rules.RedToWhiteConverterRule;
import io.substrait.colors.systems.white.rules.WhiteJoinRule;
import io.substrait.colors.systems.white.rules.WhiteProjectRule;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;

/**
 * Organizes the various {@link RelOptRule}s used when optimizing queries for the RED, GREEN, BLUE
 * and WHITE
 */
public class ColorsRules {

  public static final List<RelOptRule> RED_RULES = List.of(RedJoinRule.INSTANCE);

  public static final List<RelOptRule> GREEN_RULES = List.of();

  public static final List<RelOptRule> BLUE_RULES = List.of();

  /** Optimization rules associated with WHITE */
  public static final List<RelOptRule> WHITE_RULES =
      List.of(
          // Allows the planner to insert a relation that converts from RED to WHITE
          RedToWhiteConverterRule.INSTANCE,
          // Allows the planner to insert a relation that converts from GREEN to WHITE
          GreenToWhiteConverterRule.INSTANCE,
          // Allows the planner to insert a relation that converts from BLUE to WHITE
          BlueToWhiteConverterRule.INSTANCE,
          // Conversion rules from Convention.NONE (i.e. Logical) to the WHITE convention
          WhiteJoinRule.INSTANCE,
          WhiteProjectRule.INSTANCE);
}
