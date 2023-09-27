package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import org.snappy.copy.ToObjectRow
import org.snappy.mssql.SmallDateTime

/**
 * Custom implementation of [ISQLServerBulkData] for [Sequence]s. Continuously iterates over a
 * [Sequence] that returns elements of [ToObjectRow]. Uses the [Iterator] that a [Sequence] returns
 * (by calling [Sequence.iterator]) to continuously pull from the [Sequence] and provide those
 * values when [getRowData] is called. This allows for any lazy yielding operation to perform bulk
 * inserts by simply expressing the yielding as a [Sequence].
 */
internal class SequenceBulkCopy<R : ToObjectRow>(
    sequence: Sequence<R>,
    private val metadata: Map<Int, BulkCopyColumnMetadata>,
) : ISQLServerBulkData {
    /** Backing [Iterator] of the [Sequence] provided */
    private val iterator = sequence.iterator()
    /** [Set] of ordinals specified by the [metadata] provided */
    private val columnOrdinals: Set<Int> = metadata.keys.toSet()

    override fun getColumnOrdinals(): Set<Int> = columnOrdinals

    override fun getColumnName(column: Int): String = metadata[column]?.name
        ?: error("Cannot find metadata for column $column")

    override fun getColumnType(column: Int): Int = metadata[column]?.type
        ?: error("Cannot find metadata for column $column")

    override fun getPrecision(column: Int): Int = metadata[column]?.precision
        ?: error("Cannot find metadata for column $column")

    override fun getScale(column: Int): Int = metadata[column]?.scale
        ?: error("Cannot find metadata for column $column")

    override fun getRowData(): Array<Any?> {
        val row = iterator.next().toObjectRow()
        return Array(row.size) {
            // Not all types are made equal, smalldatetime needs a special case for bulk copy
            when (val value = row[it]) {
                is SmallDateTime -> value.bulkCopyString()
                else -> value
            }
        }
    }

    override fun next(): Boolean = iterator.hasNext()
}
