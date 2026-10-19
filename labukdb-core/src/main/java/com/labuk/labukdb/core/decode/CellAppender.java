package com.labuk.labukdb.core.decode;

import org.apache.parquet.example.data.Group;

/**
 * Appends one column's value, for one row, into the matching Arrow vector
 * at a given destination index. Two implementations:
 *
 * <ul>
 *   <li>{@link RowAppender} — physical Parquet columns; reads a different
 *       value per row from a decoded {@code Group}.</li>
 *   <li>{@link PartitionValueAppender} — Hive-style partition columns;
 *       writes the same constant for every row in a file, since a file's
 *       partition value never varies row to row. The {@code Group}
 *       parameter is ignored in this case — the value doesn't come from
 *       the row at all.</li>
 * </ul>
 *
 * Having both behind one interface is what lets {@code ParquetToArrowReader}
 * build one flat list of column fillers, in the requested output order,
 * without caring at the call site which kind of column each one is.
 */
interface CellAppender {
    void append(Group row, int destIndex);
}
