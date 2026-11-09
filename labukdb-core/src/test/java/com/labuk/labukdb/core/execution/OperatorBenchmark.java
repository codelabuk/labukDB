package com.labuk.labukdb.core.execution;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Hand-run only (a {@code main()}, not a {@code @Test}) -- measures
 * {@link AggregateOperator}/{@link HashJoinOperator} wall-clock time over a
 * single {@code BigIntVector} key at increasing row counts, to replace
 * "boxing probably costs something" with an actual before/after number.
 * Not part of {@code mvn test}; run directly, e.g.
 * {@code mvn -pl labukdb-core test-compile exec:java -Dexec.mainClass=...}
 * or via an IDE.
 */
public final class OperatorBenchmark {

    public static void main(String[] args) {
        for (int rows : new int[] {100_000, 1_000_000, 5_000_000}) {
            benchmarkAggregate(rows);
            benchmarkJoin(rows);
        }
    }

    private static void benchmarkAggregate(int rows) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field amount = new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(List.of(key, amount)), allocator)) {
            batch.allocateNew();
            BigIntVector keyVector = (BigIntVector) batch.getVector("key");
            BigIntVector amountVector = (BigIntVector) batch.getVector("amount");
            int groupCount = Math.max(1, rows / 10); // ~10 rows per group
            for (int i = 0; i < rows; i++) {
                keyVector.setSafe(i, i % groupCount);
                amountVector.setSafe(i, i);
            }
            batch.setRowCount(rows);

            long start = System.nanoTime();
            try (VectorSchemaRoot result = new AggregateOperator().aggregate(
                    List.of(batch), List.of("key"),
                    List.of(new AggregateSpec("amount", AggregateFunction.SUM, "total")),
                    allocator)) {
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                System.out.printf("AggregateOperator rows=%,d groups=%,d -> %,d ms (%,d result rows)%n",
                        rows, groupCount, elapsedMs, result.getRowCount());
            }
        }
    }

    private static void benchmarkJoin(int rows) {
        Field leftKey = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field rightKey = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot left = VectorSchemaRoot.create(new Schema(List.of(leftKey)), allocator);
             VectorSchemaRoot right = VectorSchemaRoot.create(new Schema(List.of(rightKey)), allocator)) {
            left.allocateNew();
            right.allocateNew();
            BigIntVector leftVector = (BigIntVector) left.getVector("key");
            BigIntVector rightVector = (BigIntVector) right.getVector("key");
            for (int i = 0; i < rows; i++) {
                leftVector.setSafe(i, i); // unique keys -> a 1:1 join, no fan-out noise in the timing
                rightVector.setSafe(i, i);
            }
            left.setRowCount(rows);
            right.setRowCount(rows);

            long start = System.nanoTime();
            try (VectorSchemaRoot result = new HashJoinOperator().join(JoinType.INNER,
                    left, List.of("key"), "a", right, List.of("key"), "b", false, allocator)) {
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                System.out.printf("HashJoinOperator  rows=%,d (1:1)    -> %,d ms (%,d result rows)%n",
                        rows, elapsedMs, result.getRowCount());
            }
        }
    }

    private OperatorBenchmark() {
    }
}
