package io.substrait.colors.systems.white.rules;

import io.substrait.colors.systems.white.WhiteJoin;
import io.substrait.colors.systems.white.WhiteRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WhiteJoinRule extends WhiteConverterRule {

  public static final WhiteJoinRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              LogicalJoin.class,
              Convention.NONE,
              WhiteRel.CONVENTION,
              WhiteJoinRule.class.getName())
          .withRuleFactory(WhiteJoinRule::new)
          .toRule(WhiteJoinRule.class);

  protected WhiteJoinRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;

    RelNode leftInput = join.getLeft();
    RelNode left = convert(leftInput, leftInput.getTraitSet().replace(WhiteRel.CONVENTION));

    RelNode rightInput = join.getRight();
    RelNode right = convert(rightInput, rightInput.getTraitSet().replace(WhiteRel.CONVENTION));

    return WhiteJoin.create(left, right, join.getCondition(), join.getJoinType());
  }
}
