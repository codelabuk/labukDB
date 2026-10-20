package com.labuk.labukdb.core.execution;

import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code ORDER BY} (one or more keys, independently ascending/descending)
 * + {@code LIMIT}, built on {@code arrow-algorithm}'s real sort
 * infrastructure ({@link IndexSorter}, {@link DefaultVectorComparators})
 * rather than hand-rolled comparison logic. Produces a sorted permutation
 * of row indices first, then projects only the top {@code limit} of them
 * into the output — the input is never reordered in place.
 */
public final class SortLimitOperator {

    public VectorSchemaRoot sortAndLimit(VectorSchemaRoot input, List<SortKey> sortKeys, long limit,
                                          BufferAllocator allocator) {
        int rowCount = input.getRowCount();

        try (IntVector indices = new IntVector("indices", allocator)) {
            indices.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                indices.set(i, i);
            }
            indices.setValueCount(rowCount);

            if (!sortKeys.isEmpty() && rowCount > 1) {
                List<VectorValueComparator<?>> comparators = new ArrayList<>();
                List<Boolean> ascending = new ArrayList<>();
                for (SortKey key : sortKeys) {
                    FieldVector vector = input.getVector(key.column());
                    VectorValueComparator<?> comparator = attachedComparatorFor(vector);
                    comparators.add(comparator);
                    ascending.add(key.ascending());
                }
                CompositeKeyComparator combined = new CompositeKeyComparator(comparators, ascending);
                // IndexSorter<ValueVector>.sort needs *a* vector argument matching the
                // comparator's declared generic type, but the composite comparator
                // ignores it — every sub-comparator is already attached to its own
                // column's vector individually, which is what actually drives compare().
                new IndexSorter<ValueVector>().sort(input.getVector(sortKeys.get(0).column()), indices, combined);
            }

            long effectiveLimit = limit <= 0 ? rowCount : Math.min(limit, rowCount);
            return project(input, indices, (int) effectiveLimit, allocator);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private VectorValueComparator<?> attachedComparatorFor(FieldVector vector) {
        VectorValueComparator comparator = DefaultVectorComparators.createDefaultComparator(vector);
        comparator.attachVector(vector);
        return comparator;
    }

    private VectorSchemaRoot project(VectorSchemaRoot input, IntVector sortedIndices, int limit, BufferAllocator allocator) {
        VectorSchemaRoot result = VectorSchemaRoot.create(input.getSchema(), allocator);
        result.allocateNew();
        for (int outRow = 0; outRow < limit; outRow++) {
            int sourceRow = sortedIndices.get(outRow);
            for (FieldVector outVector : result.getFieldVectors()) {
                FieldVector sourceVector = input.getVector(outVector.getField().getName());
                outVector.copyFromSafe(sourceRow, outRow, sourceVector);
            }
        }
        result.setRowCount(limit);
        return result;
    }

    /**
     * Multiple sort keys, each independently ascending/descending — not
     * directly supported by {@code arrow-algorithm}'s own
     * {@code CompositeVectorComparator} (no per-key direction in its
     * constructor), so this composes {@code DefaultVectorComparators}
     * results itself rather than fighting that class into a shape it
     * doesn't support.
     */
    private static final class CompositeKeyComparator extends VectorValueComparator<ValueVector> {
        private final List<VectorValueComparator<?>> comparators;
        private final List<Boolean> ascending;

        CompositeKeyComparator(List<VectorValueComparator<?>> comparators, List<Boolean> ascending) {
            this.comparators = comparators;
            this.ascending = ascending;
        }

        @Override
        public int compareNotNull(int index1, int index2) {
            for (int i = 0; i < comparators.size(); i++) {
                int cmp = comparators.get(i).compare(index1, index2);
                if (!ascending.get(i)) {
                    cmp = -cmp;
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        @Override
        public VectorValueComparator<ValueVector> createNew() {
            List<VectorValueComparator<?>> cloned = new ArrayList<>();
            for (VectorValueComparator<?> c : comparators) {
                cloned.add(c.createNew());
            }
            return new CompositeKeyComparator(cloned, ascending);
        }
    }
}
