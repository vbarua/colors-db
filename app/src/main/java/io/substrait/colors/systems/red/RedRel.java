package io.substrait.colors.systems.red;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/** Marks a {@link RelNode} as belonging to RED */
public interface RedRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("RED", RedRel.class);
}
