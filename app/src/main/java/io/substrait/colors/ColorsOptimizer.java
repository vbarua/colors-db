package io.substrait.colors;

import com.vbarua.isthmus.SqlToCalcite;
import io.substrait.colors.systems.white.WhiteRel;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.relation.Rel;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;

public class ColorsOptimizer {

  // Type System Configuration
  static final RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;
  static final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

  // Catalog Reader Configuration
  static final CalciteSchema calciteSchema = ColorsCatalog.getCalciteSchema(typeFactory);
  static final CalciteConnectionConfig calciteConnectionConfig =
      CalciteConnectionConfig.DEFAULT.set(
          CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString());
  static final List<String> defaultSchema = Collections.emptyList();
  static final Prepare.CatalogReader catalogReader =
      new CalciteCatalogReader(calciteSchema, defaultSchema, typeFactory, calciteConnectionConfig);

  // Calcite Builder Configuration
  static final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  // Substrait Configuration
  static final SimpleExtension.ExtensionCollection extensions;

  static {
    try {
      extensions = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static final SubstraitToCalcite substraitToCalcite =
      new SubstraitToCalcite(extensions, typeFactory);

  static final SubstraitRelVisitor calciteToSubstrait =
      new SubstraitRelVisitor(typeFactory, extensions);

  static final List<RelOptRule> RULES = ColorsRules.WHITE_RULES;
  static final Program PROGRAM =
      Programs.sequence(
          // Remove subqueries
          Programs.SUB_QUERY_PROGRAM,
          // Apply Colors Rules
          Programs.of(RuleSets.ofList(RULES)));

  private ColorsOptimizer() {}

  public static RelRoot parseFromSql(String query) {
    return parseFromSql(query, getPlanner());
  }

  private static RelRoot parseFromSql(String query, RelOptPlanner planner) {
    RelOptCluster relOptCluster = RelOptCluster.create(planner, rexBuilder);
    var sqlToCalcite = new SqlToCalcite(catalogReader, relOptCluster);
    return sqlToCalcite.convertSelect(query);
  }

  private static RelOptPlanner getPlanner() {
    var planner = new VolcanoPlanner();
    // The Convention trait is used by the Calcite optimizer to track which system a relation is
    // executed on.
    // All Logical relations have it set to NONE, as they are not executable on any system.
    // Red relations set it to RED, as those nodes are executable on the RED system. Green relations
    // set it to GREEN, BLUE relations to BLUE, and White relations to WHITE.
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    // The Collation trait is used to track the physical order of rows (i.e. sorts)
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    return planner;
  }

  public static RelNode optimizeFromSQL(String query) {
    var planner = getPlanner();
    RelRoot logicalCalciteRoot = parseFromSql(query, planner);
    RelNode logicalCalciteRel = logicalCalciteRoot.project();
    RelNode physicalPlan = optimize(planner, logicalCalciteRel);
    return physicalPlan;
  }

  public static RelNode optimize(RelOptPlanner planner, RelNode logicalPlan) {
    // The aim is to run the input plan on WHITE
    var desiredOutputTraits =
        logicalPlan
            .getTraitSet()
            // Our desired output traits are the same as those of the logical plan input, except for
            // the Convention
            .replace(WhiteRel.CONVENTION);
    planner.setRoot(logicalPlan);
    // We don't have any materializations or lattices for the optimzer to use
    List<RelOptMaterialization> materializations = Collections.emptyList();
    List<RelOptLattice> lattices = Collections.emptyList();
    PROGRAM.run(planner, logicalPlan, desiredOutputTraits, materializations, lattices);
    return planner.findBestExp();
  }

  public static Rel toSubstrait(RelNode physicalPlan) {
    // TODO
    Rel optimizedSubstraitRel = calciteToSubstrait.apply(physicalPlan);
    return null;
  }
}
