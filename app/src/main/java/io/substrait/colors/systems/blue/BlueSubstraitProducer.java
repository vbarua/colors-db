package io.substrait.colors.systems.blue;

import io.substrait.expression.Expression;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Reduce a Calcite {@link RelNode} tree of {@link BlueRel}s into a Substrait {@link NamedScan} */
public class BlueSubstraitProducer implements BlueVisitor<Void> {

  public static Rel produce(
      RelNode rel, RexExpressionConverter rexExpressionConverter, TypeConverter typeConverter) {
    BlueRel blue = (BlueRel) rel;
    var producer = new BlueSubstraitProducer();
    blue.accept(producer);

    // The schema of the Substrait NamedScan is that of the table
    NamedStruct substraitSchema = typeConverter.toNamedStruct(producer.table.getRowType());

    // Create a filter expression if one is present
    Optional<Expression> substraitFilter =
        switch (producer.predicates.size()) {
          case 0 -> Optional.empty();
          case 1 -> Optional.of(producer.predicates.getFirst().accept(rexExpressionConverter));
          default -> {
            RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            RexNode calciteCall = rexBuilder.makeCall(SqlStdOperatorTable.AND, producer.predicates);
            yield Optional.of(calciteCall.accept(rexExpressionConverter));
          }
        };

    return NamedScan.builder()
        .addAllNames(producer.table.getQualifiedName())
        .initialSchema(substraitSchema)
        .filter(substraitFilter)
        .build();
  }

  private RelOptTable table;
  private final List<RexNode> predicates = new ArrayList<>();

  @Override
  public Void visit(BlueFilter filter) {
    ((BlueRel) filter.getInput()).accept(this);
    // Keep track of all predicates encountered
    predicates.addAll(RelOptUtil.conjunctions(filter.getCondition()));
    return null;
  }

  @Override
  public Void visit(BlueTableScan scan) {
    // The BLUE subtree can only have a single-leaf, which must be a TableScan
    // Store the table for conversion to Substrait
    this.table = scan.getTable();
    return null;
  }
}
