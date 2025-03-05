package io.substrait.colors.systems.red.rules;

import io.substrait.colors.systems.red.RedProject;
import io.substrait.colors.systems.red.RedRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RedProjectRule extends RedConverterRule {

  public static final RedProjectRule INSTANCE =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              LogicalProject.class,
              Convention.NONE,
              RedRel.CONVENTION,
              RedProjectRule.class.getName())
          .withRuleFactory(RedProjectRule::new)
          .toRule(RedProjectRule.class);

  protected RedProjectRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    final Project project = (Project) rel;
    RelNode input =
        convert(project.getInput(), project.getInput().getTraitSet().replace(RedRel.CONVENTION));
    return RedProject.create(input, project.getProjects(), project.getRowType());
  }
}
