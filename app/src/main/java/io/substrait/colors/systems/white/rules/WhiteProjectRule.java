package io.substrait.colors.systems.white.rules;

import io.substrait.colors.systems.white.WhiteProject;
import io.substrait.colors.systems.white.WhiteRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WhiteProjectRule extends WhiteConverterRule {

  public static final WhiteProjectRule INSTANCE =
      Config.INSTANCE
          .withConversion(
              LogicalProject.class,
              Convention.NONE,
              WhiteRel.CONVENTION,
              WhiteConverterRule.class.getName())
          .withRuleFactory(WhiteProjectRule::new)
          .toRule(WhiteProjectRule.class);

  protected WhiteProjectRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    LogicalProject project = (LogicalProject) rel;
    RelNode input = convert(project.getInput(), getOutConvention());
    return WhiteProject.create(input, project.getProjects(), project.getRowType());
  }
}
