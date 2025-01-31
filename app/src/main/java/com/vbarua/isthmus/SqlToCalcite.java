package com.vbarua.isthmus;

import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/**
 * Utility class for converting from SQL to Substrait
 *
 * <p>Can be extended to modify the default configurations used during the conversion
 */
public class SqlToCalcite {

  protected final Prepare.CatalogReader catalogReader;
  protected final RelOptCluster relOptCluster;

  public SqlToCalcite(Prepare.CatalogReader catalogReader, RelOptCluster relOptCluster) {
    this.catalogReader = catalogReader;
    this.relOptCluster = relOptCluster;
  }

  public RelRoot convertSelect(String selectQuery) {
    // Convert SQL Select string to SQL AST
    SqlParser parser = SqlParser.create(selectQuery, getSqlParserConfig());
    SqlNode sqlTree;
    try {
      sqlTree = parser.parseQuery();
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }

    // Convert SQL AST to Calcite Relational Tree
    boolean needsValidation = true;
    boolean top = true;
    SqlToRelConverter sqlToRelConverter = getSqlToRelConverter();
    return sqlToRelConverter.convertQuery(sqlTree, true, true);
  }

  protected SqlToRelConverter getSqlToRelConverter() {
    return new SqlToRelConverter(
        getViewExpander(),
        getSqlValidator(),
        this.catalogReader,
        this.relOptCluster,
        getSqlRexConvertletTable(),
        getSqlToRelConfig());
  }

  protected SqlParser.Config getSqlParserConfig() {
    return SqlParser.config().withUnquotedCasing(Casing.UNCHANGED);
  }

  protected SqlToRelConverter.Config getSqlToRelConfig() {
    return SqlToRelConverter.config();
  }

  protected SqlValidator.Config getSqlValidatorConfig() {
    return SqlValidator.Config.DEFAULT;
  }

  protected SqlOperatorTable getOperatorTable() {
    return SubstraitOperatorTable.INSTANCE;
  }

  protected SqlValidator getSqlValidator() {
    return SqlValidatorUtil.newValidator(
        getOperatorTable(), catalogReader, relOptCluster.getTypeFactory(), getSqlValidatorConfig());
  }

  protected RelOptTable.ViewExpander getViewExpander() {
    return null;
  }

  protected SqlRexConvertletTable getSqlRexConvertletTable() {
    return StandardConvertletTable.INSTANCE;
  }
}
