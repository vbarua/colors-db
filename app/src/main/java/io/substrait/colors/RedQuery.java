package io.substrait.colors;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.relation.Extension;
import io.substrait.type.Type;

public class RedQuery implements Extension.LeafRelDetail {

  private final Type.Struct rowType;
  private final String sqlQuery;

  public RedQuery(String sqlQuery, Type.Struct rowType) {
    this.sqlQuery = sqlQuery;
    this.rowType = rowType;
  }

  @Override
  public Type.Struct deriveRecordType() {
    return rowType;
  }

  @Override
  public Any toProto() {
    // TODO: use an actual message for this
    StringValue stringValue = StringValue.newBuilder().setValue(sqlQuery).build();
    return Any.pack(stringValue);
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
