package io.substrait.colors;

import io.substrait.colors.systems.blue.BlueSubstraitProducer;
import io.substrait.colors.systems.red.RedSubstraitProducer;
import io.substrait.colors.systems.white.BlueToWhiteConverter;
import io.substrait.colors.systems.white.GreenToWhiteConverter;
import io.substrait.colors.systems.white.RedToWhiteConverter;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.relation.Rel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;

/** Extends the standard {@link SubstraitRelVisitor} to handle ColorsDB specific {@link RelNode}s */
public class ColorsSubstraitProducer extends SubstraitRelVisitor {

  public ColorsSubstraitProducer(
      RelDataTypeFactory typeFactory, SimpleExtension.ExtensionCollection extensions) {
    super(typeFactory, extensions);
  }

  @Override
  public Rel visitOther(RelNode other) {
    if (other instanceof RedToWhiteConverter redToWhiteConverter) {
      return visit(redToWhiteConverter);
    } else if (other instanceof GreenToWhiteConverter greenToWhiteConverter) {
      return visit(greenToWhiteConverter);
    } else if (other instanceof BlueToWhiteConverter blueToWhiteConverter) {
      return visit(blueToWhiteConverter);
    }
    return super.visitOther(other);
  }

  protected Rel visit(RedToWhiteConverter redToWhiteConverter) {
    return RedSubstraitProducer.produce(redToWhiteConverter.getInput(), typeConverter);
  }

  protected Rel visit(BlueToWhiteConverter blueToWhiteConverter) {
    return BlueSubstraitProducer.produce(
        blueToWhiteConverter.getInput(), rexExpressionConverter, typeConverter);
  }

  protected Rel visit(GreenToWhiteConverter greenToWhiteConverter) {
    throw new RuntimeException("Handle GREEN");
  }
}
