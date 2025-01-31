package io.substrait.colors.systems.white;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/** Marks a {@link RelNode} as belonging to WHITE */
public interface WhiteRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("WHITE", WhiteRel.class);
}
