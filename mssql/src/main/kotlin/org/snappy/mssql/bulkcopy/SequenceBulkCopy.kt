package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import org.snappy.copy.ToObjectRow
import org.snappy.mssql.SmallDateTime

internal class SequenceBulkCopy<R : ToObjectRow>(
    sequence: Sequence<R>,
    private val metadata: Map<Int, BulkCopyColumnMetadata>,
) : ISQLServerBulkData {
    private val iterator = sequence.iterator()
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
            when (val value = row[it]) {
                is SmallDateTime -> value.bulkCopyString()
                else -> value
            }
        }
    }

    override fun next(): Boolean = iterator.hasNext()
}
