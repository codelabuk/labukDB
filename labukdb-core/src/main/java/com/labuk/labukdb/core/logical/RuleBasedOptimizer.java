package com.labuk.labukdb.core.logical;

import java.util.List;

/**
 * Applies each rule once, in order — no fixed-point iteration needed yet;
 * Phase 1's two rules don't create new opportunities for each other to
 * re-fire ({@code PushFilterIntoScan} runs first so {@code
 * PushProjectIntoScan}'s needed-column walk sees the final tree shape).
 */
public final class RuleBasedOptimizer {

    public static final RuleBasedOptimizer DEFAULT = new RuleBasedOptimizer(
            List.of(new PushFilterIntoScan(), new PushProjectIntoScan()));

    private final List<OptimizerRule> rules;

    public RuleBasedOptimizer(List<OptimizerRule> rules) {
        this.rules = List.copyOf(rules);
    }

    public LogicalPlan optimize(LogicalPlan plan) {
        LogicalPlan current = plan;
        for (OptimizerRule rule : rules) {
            current = rule.apply(current);
        }
        return current;
    }
}
