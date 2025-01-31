package io.substrait.colors;

import io.substrait.colors.systems.blue.BlueTableScan;
import io.substrait.colors.systems.green.GreenTableScan;
import io.substrait.colors.systems.red.RedTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * {@link Table} implementation which contains {@link Color} information for mapping the table to
 * the correct {@link TableScan} variant when the {@link TranslatableTable#toRel} method is called
 */
public class ColorsTable extends AbstractTable implements TranslatableTable {

  public enum Color {
    RED,
    GREEN,
    BLUE
  }

  private final Color color;
  private final RelDataType rowType;

  public ColorsTable(Color color, RelDataType rowType) {
    this.color = color;
    this.rowType = rowType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    RelOptCluster cluster = context.getCluster();
    return switch (color) {
      case RED -> RedTableScan.create(cluster, relOptTable);
      case GREEN -> GreenTableScan.create(cluster, relOptTable);
      case BLUE -> BlueTableScan.create(cluster, relOptTable);
    };
  }
}
