package io.substrait.colors.systems.blue;

import java.util.Collections;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

/** {@link RelNode} which represents a {@link TableScan} executed in BLUE */
public class BlueTableScan extends TableScan implements BlueRel {

  private BlueTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, Collections.emptyList(), table);
    assert getConvention() == CONVENTION;
  }

  public static BlueTableScan create(RelOptCluster cluster, RelOptTable table) {
    final RelTraitSet traitSet = cluster.traitSetOf(CONVENTION);
    return new BlueTableScan(cluster, traitSet, table);
  }

  @Override
  public <T> T accept(BlueVisitor<T> blueVisitor) {
    return blueVisitor.visit(this);
  }
}
