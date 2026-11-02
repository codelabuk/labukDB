package com.labuk.labukdb.core.logical;

import com.labuk.labukdb.core.execution.SortKey;

import java.util.List;

/** An {@code ORDER BY} node. Reuses the existing {@link SortKey} shape directly. */
public record Sort(LogicalPlan input, List<SortKey> sortKeys) implements LogicalPlan {
}
