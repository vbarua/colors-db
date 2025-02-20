package io.substrait.colors.systems.blue.rules;

import io.substrait.colors.systems.blue.BlueFilter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

/**
 * Rule which splits a {@link LogicalFilter} with multiple predicates into two {@link
 * LogicalFilter}s, one that can be executed in BLUE, and one that cannot.
 *
 * <p>Along with {@link BlueFilterRule} allows for better pushdown of filters into BLUE.
 */
@Value.Enclosing
public class BlueFilterSplitRule extends RelRule<BlueFilterSplitRule.Config>
    implements TransformationRule {

  protected BlueFilterSplitRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    List<RexNode> predicates = RelOptUtil.conjunctions(filter.getCondition());
    List<RexNode> bluePredicates = new ArrayList<>();
    List<RexNode> otherPredicates = new ArrayList<>();
    for (RexNode predicate : predicates) {
      if (BlueFilter.validPredicate(predicate)) {
        bluePredicates.add(predicate);
      } else {
        otherPredicates.add(predicate);
      }
    }

    // Build the new filter
    RelBuilder relBuilder = call.builder();
    relBuilder.push(filter.getInput());
    relBuilder.filter(bluePredicates);
    relBuilder.filter(otherPredicates);
    RelNode splitFilter = relBuilder.build();
    call.transformTo(splitFilter);
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {

    Config DEFAULT =
        ImmutableBlueFilterSplitRule.Config.builder()
            .build()
            .withOperandSupplier(
                b ->
                    b.operand(Filter.class)
                        //
                        .predicate(
                            f ->
                                !BlueFilter.validFilter(f)
                                    && RelOptUtil.conjunctions(f.getCondition()).stream()
                                        .anyMatch(BlueFilter::validPredicate))
                        .anyInputs());

    @Override
    default BlueFilterSplitRule toRule() {
      return new BlueFilterSplitRule(this);
    }
  }
}
