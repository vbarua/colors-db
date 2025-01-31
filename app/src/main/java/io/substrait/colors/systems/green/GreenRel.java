package io.substrait.colors.systems.green;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/** Marks a {@link RelNode} as belonging to GREEN */
public interface GreenRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("GREEN", GreenRel.class);
}
