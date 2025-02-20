package io.substrait.colors.systems.red;

import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.ImmutableExtensionLeaf;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

/** Reduce a Calcite {@link RelNode} tree of {@link RedRel}s into a {@link RedQuery} */
public class RedSubstraitProducer {

  public static Rel produce(RelNode rel, TypeConverter typeConverter) {
    RedRel red = (RedRel) rel;

    // Use Calcite tooling to convert the RED subtree into a SQL query
    RelToSqlConverter relToSql = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    SqlImplementor.Result result = relToSql.visitRoot(red);
    SqlNode statement = result.asStatement();

    // Encode the query into an ExtensionLeaf
    Type.Struct schema = (Type.Struct) typeConverter.toSubstrait(red.getRowType());
    RedQuery redQuery = new RedQuery(statement.toString(), schema);
    return ImmutableExtensionLeaf.builder().deriveRecordType(schema).detail(redQuery).build();
  }
}
