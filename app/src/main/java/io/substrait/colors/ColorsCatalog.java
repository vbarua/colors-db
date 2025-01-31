package io.substrait.colors;

import com.vbarua.isthmus.SubstraitSchema;
import io.substrait.colors.utils.ColorsSchemaParser;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;

/**
 * Utility class for converting the various schemas (red_schema.sql, green_schema.sql,
 * blue_schema.sql) into a {@link CalciteSchema} for use when parsing SQL queries
 */
public class ColorsCatalog {

  private static String asString(String resource) {
    try (InputStream sqlSchemaStream = ColorsCatalog.class.getResourceAsStream(resource)) {
      return new String(sqlSchemaStream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static CalciteSchema getCalciteSchema(RelDataTypeFactory typeFactory) {
    var rootSchema = CalciteSchema.createRootSchema(false);

    var schemaParser = new ColorsSchemaParser(typeFactory);
    // RED Schema
    Map<String, Table> redTables =
        schemaParser.convertCreates(ColorsTable.Color.RED, asString("/red_schema.sql"));
    SubstraitSchema redSchema = new SubstraitSchema(redTables);
    rootSchema.add("red", redSchema);

    // GREEN Schema
    Map<String, Table> greenTables =
        schemaParser.convertCreates(ColorsTable.Color.GREEN, asString("/green_schema.sql"));
    SubstraitSchema greenSchema = new SubstraitSchema(greenTables);
    rootSchema.add("green", greenSchema);

    // BLUE Schema
    Map<String, Table> blueTables =
        schemaParser.convertCreates(ColorsTable.Color.BLUE, asString("/blue_schema.sql"));
    SubstraitSchema blueSchema = new SubstraitSchema(blueTables);
    rootSchema.add("blue", blueSchema);

    return rootSchema;
  }
}
