package io.substrait.colors.systems.red.rules;

import io.substrait.colors.systems.red.RedJoin;
import io.substrait.colors.systems.red.RedRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RedJoinRule extends RedConverterRule {

  public static final RedJoinRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              LogicalJoin.class, Convention.NONE, RedRel.CONVENTION, RedJoinRule.class.getName())
          .withRuleFactory(RedJoinRule::new)
          .toRule(RedJoinRule.class);

  protected RedJoinRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;

    RelNode leftInput = join.getLeft();
    RelNode left = convert(leftInput, leftInput.getTraitSet().replace(RedRel.CONVENTION));

    RelNode rightInput = join.getRight();
    RelNode right = convert(rightInput, rightInput.getTraitSet().replace(RedRel.CONVENTION));

    return RedJoin.create(left, right, join.getCondition(), join.getJoinType());
  }
}
