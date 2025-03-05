package io.substrait.colors.systems.blue;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/**
 * BLUE has limited filtering capabilities.
 *
 * <p>It can apply 1 or more equality predicates where one side is a literal and another is a field
 * reference.
 */
public class BlueFilter extends Filter implements BlueRel {

  private BlueFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert traits.getConvention() == CONVENTION;
  }

  public static BlueFilter create(RelNode input, RexNode condition) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().replace(CONVENTION);
    return new BlueFilter(cluster, traitSet, input, condition);
  }

  public static boolean validFilter(Filter filter) {
    List<RexNode> predicates = RelOptUtil.conjunctions(filter.getCondition());
    return predicates.stream().allMatch(BlueFilter::validPredicate);
  }

  public static boolean validPredicate(RexNode rex) {
    if (!rex.isA(SqlKind.EQUALS)) {
      return false;
    }
    if (!(rex instanceof RexCall call)) {
      return false;
    }
    var args = call.getOperands();
    var left = args.get(0);
    var right = args.get(1);

    return
    // left is a field reference and right is a literal
    (left instanceof RexInputRef && right instanceof RexLiteral)
        ||
        // left is a literal and right is a field reference
        (left instanceof RexLiteral && right instanceof RexInputRef);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new BlueFilter(input.getCluster(), traitSet, input, condition);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(COST_SCALING);
  }

  @Override
  public <T> T accept(BlueVisitor<T> blueVisitor) {
    return blueVisitor.visit(this);
  }
}
