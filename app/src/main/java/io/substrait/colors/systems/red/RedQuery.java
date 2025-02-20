package io.substrait.colors.systems.red;

import com.google.protobuf.Any;
import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.Extension;
import io.substrait.type.Type;
import io.substrait.type.proto.TypeProtoConverter;

public class RedQuery implements Extension.LeafRelDetail {

  private final Type.Struct schema;
  private final String sqlQuery;

  public RedQuery(String sqlQuery, Type.Struct schema) {
    this.sqlQuery = sqlQuery;
    this.schema = schema;
  }

  @Override
  public Type.Struct deriveRecordType() {
    return schema;
  }

  @Override
  public Any toProto() {
    // Creating a TypeProtoConverter here is actually subtly buggy if user-defined types are being
    // used, because they will not be registered in the ExtensionCollector that is being used to
    // convert the rest of the plan
    // TODO: improve the upstream API to avoid this issue
    var typeProtoConverter = new TypeProtoConverter(new ExtensionCollector());
    var protoTypes = schema.fields().stream().map(t -> t.accept(typeProtoConverter)).toList();
    var protoSchema = io.substrait.proto.Type.Struct.newBuilder().addAllTypes(protoTypes);
    var redQuery =
        io.substrait.colors.extensions.protobuf.RedQuery.newBuilder()
            .setSqlQuery(sqlQuery)
            .setSchema(protoSchema)
            .build();
    return Any.pack(redQuery);
  }

  @Override
  public String toString() {
    var sb = new StringBuilder();
    sb.append("RedQuery{sqlQuery='");
    sb.append(sqlQuery);
    sb.append("'}");
    return sb.toString();
  }
}
