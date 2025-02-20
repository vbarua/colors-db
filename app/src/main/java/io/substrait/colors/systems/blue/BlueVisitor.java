package io.substrait.colors.systems.blue;

public interface BlueVisitor<T> {
  T visit(BlueFilter filter);

  T visit(BlueTableScan scan);
}
