package io.substrait.colors.systems.white.rules;

import io.substrait.colors.systems.blue.BlueRel;
import io.substrait.colors.systems.white.BlueToWhiteConverter;
import io.substrait.colors.systems.white.WhiteRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link ConverterRule} which transforms from BLUE to WHITE
 *
 * <p>Allows the planner to inject this transformation as needed.
 */
public class BlueToWhiteConverterRule extends WhiteConverterRule {

  public static final ConverterRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              RelNode.class,
              BlueRel.CONVENTION,
              WhiteRel.CONVENTION,
              BlueToWhiteConverterRule.class.getName())
          .withRuleFactory(BlueToWhiteConverterRule::new)
          .toRule(BlueToWhiteConverterRule.class);

  private BlueToWhiteConverterRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    return BlueToWhiteConverter.create(rel);
  }
}
