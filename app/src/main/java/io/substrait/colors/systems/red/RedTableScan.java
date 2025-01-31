package io.substrait.colors.systems.red;

import java.util.Collections;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

/** {@link RelNode} which represents a {@link TableScan} executed in RED */
public class RedTableScan extends TableScan implements RedRel {

  private RedTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, Collections.emptyList(), table);
    assert getConvention() == CONVENTION;
  }

  public static RedTableScan create(RelOptCluster cluster, RelOptTable table) {
    final RelTraitSet traitSet = cluster.traitSetOf(CONVENTION);
    return new RedTableScan(cluster, traitSet, table);
  }
}
