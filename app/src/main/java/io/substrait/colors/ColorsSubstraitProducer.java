package io.substrait.colors;

import io.substrait.colors.systems.white.BlueToWhiteConverter;
import io.substrait.colors.systems.white.GreenToWhiteConverter;
import io.substrait.colors.systems.white.RedToWhiteConverter;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.relation.Extension;
import io.substrait.relation.ImmutableExtensionLeaf;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

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
    // Calcite Time
    var redRoot = redToWhiteConverter.getInput();
    RelToSqlConverter relToSql = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    SqlImplementor.Result result = relToSql.visitRoot(redRoot);
    SqlNode statement = result.asStatement();

    // Extension Time
    Type.Struct rowType = (Type.Struct) typeConverter.toSubstrait(redRoot.getRowType());
    Extension.LeafRelDetail detail = new RedQuery(statement.toString(), rowType);
    return ImmutableExtensionLeaf.builder().detail(detail).deriveRecordType(rowType).build();
  }

  protected Rel visit(BlueToWhiteConverter blueToWhiteConverter) {
    throw new RuntimeException("Handle BLUE");
  }

  protected Rel visit(GreenToWhiteConverter greenToWhiteConverter) {
    throw new RuntimeException("Handle GREEN");
  }
}
