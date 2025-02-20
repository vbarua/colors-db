package io.substrait.colors.systems.blue;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/** Marks a {@link RelNode} as belonging to BLUE */
public interface BlueRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("BLUE", BlueRel.class);
  double BLUE_COST_SCALING = 0.01;

  <T> T accept(BlueVisitor<T> blueVisitor);
}
