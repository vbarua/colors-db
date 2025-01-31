package io.substrait.colors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ColorsOptimizerTest {

  @Nested
  class SqlParserTest {
    @Test
    void red() {
      var relRoot = ColorsOptimizer.parseFromSql("SELECT p_partkey, p_name FROM red.part");
      assertEquals(
          """
        LogicalProject(p_partkey=[$0], p_name=[$1])
          RedTableScan(table=[[red, part]])
        """,
          relRoot.project().explain());
    }

    @Test
    void green() {
      var relRoot = ColorsOptimizer.parseFromSql("SELECT o_orderkey, o_custkey FROM green.orders");
      assertEquals(
          """
          LogicalProject(o_orderkey=[$0], o_custkey=[$1])
            GreenTableScan(table=[[green, orders]])
          """,
          relRoot.project().explain());
    }

    @Test
    void blue() {
      var relRoot = ColorsOptimizer.parseFromSql("SELECT c_custkey, c_name FROM blue.customer");
      assertEquals(
          """
          LogicalProject(c_custkey=[$0], c_name=[$1])
            BlueTableScan(table=[[blue, customer]])
          """,
          relRoot.project().explain());
    }
  }

  @Nested
  class OptimizerTest {
    @Test
    void red() {
      RelNode physicalRel =
          ColorsOptimizer.optimizeFromSQL("SELECT p_partkey, p_name FROM red.part");
      assertEquals(
          """
          WhiteProject(p_partkey=[$0], p_name=[$1])
            RedToWhiteConverter
              RedTableScan(table=[[red, part]])
          """,
          physicalRel.explain());
    }
  }
}
