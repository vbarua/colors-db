package io.substrait.colors.systems.white.rules;

import io.substrait.colors.systems.green.GreenRel;
import io.substrait.colors.systems.white.GreenToWhiteConverter;
import io.substrait.colors.systems.white.WhiteRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link ConverterRule} which transforms from Green to WHITE
 *
 * <p>Allows the planner to inject this transformation as needed.
 */
public class GreenToWhiteConverterRule extends WhiteConverterRule {

  public static final ConverterRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              RelNode.class,
              GreenRel.CONVENTION,
              WhiteRel.CONVENTION,
              GreenToWhiteConverterRule.class.getName())
          .withRuleFactory(GreenToWhiteConverterRule::new)
          .toRule(GreenToWhiteConverterRule.class);

  private GreenToWhiteConverterRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    return GreenToWhiteConverter.create(rel);
  }
}
