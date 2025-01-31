package io.substrait.colors.utils;

import io.substrait.colors.ColorsTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;

/** Utility class for parsing the CREATE statements in the various *_schema.sql files */
public class ColorsSchemaParser {

  private final SqlValidatorWithHints validator;

  public ColorsSchemaParser(RelDataTypeFactory typeFactory) {
    // Use an empty schema and catalog when parsing the CREATE statements, because it's from these
    // statements that we will create the catalog
    var emptySchema = CalciteSchema.createRootSchema(false);
    var emptyCatalog =
        new CalciteCatalogReader(
            emptySchema,
            List.of(),
            typeFactory,
            CalciteConnectionConfig.DEFAULT.set(
                CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString()));
    this.validator =
        SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(), emptyCatalog, typeFactory, SqlValidator.Config.DEFAULT);
  }

  public Map<String, Table> convertCreates(ColorsTable.Color color, String createQueries) {
    SqlParser parser =
        SqlParser.create(
            createQueries,
            SqlParser.config()
                // To process CREATE statements we must use the SqlDdlParserImpl, as the default
                // parser does not handle them
                .withParserFactory(SqlDdlParserImpl.FACTORY)
                .withUnquotedCasing(Casing.TO_LOWER));
    SqlNodeList sqlNodeList;
    try {
      sqlNodeList = parser.parseStmtList();
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }

    Map<String, Table> tables = new HashMap<>();
    for (SqlNode sqlNode : sqlNodeList) {
      SqlCreateTable createTable = validateCreateTable(sqlNode);
      var columnDeclarations = validateColumnDeclarations(createTable.columnList);

      List<RelDataTypeField> fields = new ArrayList<>(columnDeclarations.size());
      for (int index = 0; index < columnDeclarations.size(); index++) {
        SqlColumnDeclaration cd = columnDeclarations.get(index);
        RelDataType type = cd.dataType.deriveType(validator);
        String name = cd.name.names.get(0);

        fields.add(new RelDataTypeFieldImpl(name, index, type));
      }
      var rowType = new RelRecordType(StructKind.FULLY_QUALIFIED, fields, false);
      tables.put(createTable.name.toString(), new ColorsTable(color, rowType));
    }
    return tables;
  }

  protected SqlCreateTable validateCreateTable(SqlNode sqlNode) {
    if (!(sqlNode instanceof SqlCreateTable createTable)) {
      throw new RuntimeException("Expected only CREATE Statements");
    }
    if (createTable.name.names.size() > 1) {
      throw new RuntimeException();
    }
    if (createTable.query != null) {
      throw new RuntimeException();
    }
    return createTable;
  }

  protected List<SqlColumnDeclaration> validateColumnDeclarations(SqlNodeList sqlNodes) {
    List<SqlColumnDeclaration> columnDeclarations = new ArrayList<>();
    for (SqlNode node : sqlNodes) {
      if (node instanceof SqlColumnDeclaration columnDeclaration) {
        if (columnDeclaration.name.names.size() != 1) {
          throw new RuntimeException();
        }
        columnDeclarations.add(columnDeclaration);
      } else if (node instanceof SqlKeyConstraint) {
        // ignore constraint declarations
        continue;
      } else {
        throw new RuntimeException();
      }
    }
    return columnDeclarations;
  }
}
