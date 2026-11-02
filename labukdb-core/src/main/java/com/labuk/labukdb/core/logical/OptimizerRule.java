package com.labuk.labukdb.core.logical;

/** One rewrite step a {@link RuleBasedOptimizer} applies to a whole plan tree. */
public interface OptimizerRule {
    LogicalPlan apply(LogicalPlan plan);
}
