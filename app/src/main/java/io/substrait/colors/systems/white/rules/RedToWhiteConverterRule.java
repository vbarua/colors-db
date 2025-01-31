package io.substrait.colors.systems.white.rules;

import io.substrait.colors.systems.red.RedRel;
import io.substrait.colors.systems.white.RedToWhiteConverter;
import io.substrait.colors.systems.white.WhiteRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link ConverterRule} which transforms from RED to WHITE
 *
 * <p>Allows the planner to inject this transformation as needed.
 */
public class RedToWhiteConverterRule extends WhiteConverterRule {

  public static final ConverterRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              RelNode.class,
              RedRel.CONVENTION,
              WhiteRel.CONVENTION,
              RedToWhiteConverterRule.class.getName())
          .withRuleFactory(RedToWhiteConverterRule::new)
          .toRule(RedToWhiteConverterRule.class);

  private RedToWhiteConverterRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    return RedToWhiteConverter.create(rel);
  }
}
