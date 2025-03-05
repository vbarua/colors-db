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
      var optimizer = new ColorsOptimizer();
      var relRoot = optimizer.parseFromSql("SELECT p_partkey, p_name FROM red.part");
      assertEquals(
          """
          LogicalProject(p_partkey=[$0], p_name=[$1])
            RedTableScan(table=[[red, part]])
          """,
          relRoot.project().explain());
    }

    @Test
    void green() {
      var optimizer = new ColorsOptimizer();
      var relRoot = optimizer.parseFromSql("SELECT o_orderkey, o_custkey FROM green.orders");
      assertEquals(
          """
          LogicalProject(o_orderkey=[$0], o_custkey=[$1])
            GreenTableScan(table=[[green, orders]])
          """,
          relRoot.project().explain());
    }

    @Test
    void blue() {
      var optimizer = new ColorsOptimizer();
      var relRoot = optimizer.parseFromSql("SELECT c_custkey, c_name FROM blue.customer");
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
      var optimizer = new ColorsOptimizer();
      RelNode physicalRel = optimizer.optimizeFromSQL("SELECT p_partkey, p_name FROM red.part");
      assertEquals(
          """
          RedToWhiteConverter
            RedProject(p_partkey=[$0], p_name=[$1])
              RedTableScan(table=[[red, part]])
          """,
          physicalRel.explain());
    }

    @Test
    void redJoin() {
      var optimizer = new ColorsOptimizer();
      var query =
          """
              SELECT ps.ps_partkey
              FROM red.supplier AS s
              JOIN red.partsupp AS ps
                ON ps.ps_suppkey = s.s_suppkey
              WHERE ps.ps_availqty < 5
              """;
      RelNode physicalRel = optimizer.optimizeFromSQL(query);
      assertEquals(
          """
          RedToWhiteConverter
            RedProject(ps_partkey=[$7])
              RedJoin(condition=[=($8, $0)], joinType=[inner])
                RedTableScan(table=[[red, supplier]])
                RedFilter(condition=[<($2, 5)])
                  RedTableScan(table=[[red, partsupp]])
              """,
          physicalRel.explain());
    }

    @Test
    void blueFilter() {
      var optimizer = new ColorsOptimizer();
      RelNode physicalRel =
          optimizer.optimizeFromSQL(
              "SELECT c_name FROM blue.customer WHERE c_nationkey=5 AND c_name > 'g'");
      assertEquals(
          """
          WhiteProject(c_name=[$1])
            WhiteFilter(condition=[>($1, 'g')])
              BlueToWhiteConverter
                BlueFilter(condition=[=($3, 5)])
                  BlueTableScan(table=[[blue, customer]])
          """,
          physicalRel.explain());
    }

    @Test
    void redBlueJoin() {
      var optimizer = new ColorsOptimizer();
      var query =
          """
          SELECT ps.ps_partkey, s_suppkey, s_name, n_name
          FROM red.supplier AS s
          JOIN red.partsupp AS ps
            ON ps.ps_suppkey = s.s_suppkey
          JOIN blue.nation AS n
            ON n.n_nationkey = s.s_nationkey
          WHERE ps.ps_availqty < 5
          """;
      RelNode physicalRel = optimizer.optimizeFromSQL(query);
      assertEquals(
          """
          WhiteProject(ps_partkey=[$7], s_suppkey=[$0], s_name=[$1], n_name=[$13])
            WhiteJoin(condition=[=($12, $3)], joinType=[inner])
              RedToWhiteConverter
                RedJoin(condition=[=($8, $0)], joinType=[inner])
                  RedTableScan(table=[[red, supplier]])
                  RedFilter(condition=[<($2, 5)])
                    RedTableScan(table=[[red, partsupp]])
              BlueToWhiteConverter
                BlueTableScan(table=[[blue, nation]])
              """,
          physicalRel.explain());
    }

    @Test
    void whiteJoin() {
      var optimizer = new ColorsOptimizer();
      var query =
          """
          SELECT o_orderkey, c_custkey
          FROM green.orders o
          JOIN blue.customer c
            ON o.o_custkey = c.c_custkey
          """;
      RelNode physicalRel = optimizer.optimizeFromSQL(query);
      assertEquals(
          """
          WhiteProject(o_orderkey=[$0], c_custkey=[$9])
            WhiteJoin(condition=[=($1, $9)], joinType=[inner])
              GreenToWhiteConverter
                GreenTableScan(table=[[green, orders]])
              BlueToWhiteConverter
                BlueTableScan(table=[[blue, customer]])
          """,
          physicalRel.explain());
    }
  }
}
