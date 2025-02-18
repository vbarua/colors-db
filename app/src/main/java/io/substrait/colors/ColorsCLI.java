package io.substrait.colors;

import io.substrait.relation.Rel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;

public class ColorsCLI {

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new RuntimeException("Expected a single query as input");
    }
    String query = args[0];

    ColorsOptimizer optimizer = new ColorsOptimizer();

    System.out.println("=== Logical Calcite Plan ===");
    RelRoot logicalCalcitePlanRoot = optimizer.parseFromSql(query);
    RelNode logicalCalcitePlan = logicalCalcitePlanRoot.project();
    System.out.println(logicalCalcitePlan.explain());

    System.out.println("=== Physical Calcite Plan ===");
    RelNode physicalCalcitePlan = optimizer.optimize(logicalCalcitePlan);
    System.out.println(physicalCalcitePlan.explain());

    System.out.println("=== Substrait Plan ===");
    Rel substraitPlan = optimizer.toSubstrait(physicalCalcitePlan);
    System.out.println(substraitPlan.toString());
  }
}
