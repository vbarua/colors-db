package io.substrait.colors.systems.blue;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/** Marks a {@link RelNode} as belonging to BLUE */
public interface BlueRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("BLUE", BlueRel.class);
}
