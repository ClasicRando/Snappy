package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import org.snappy.copy.ToObjectRow

internal class SequenceBulkData<R : ToObjectRow>(
    private val destinationTable: String,
    sequence: Sequence<R>,
) : ISQLServerBulkData {
    private val iterator = sequence.iterator()
    private val columnOrdinals: MutableSet<Int> = mutableSetOf()
    private val metadata: MutableMap<Int, BulkCopyColumnMetadata> = mutableMapOf()

    internal fun fetchMetadata(connection: ISQLServerConnection) {
        connection.metaData.getColumns(
            null,
            null,
            destinationTable,
            null,
        ).use { rs ->
            while (rs.next()) {
                val ordinal = rs.getInt("ORDINAL_POSITION")
                columnOrdinals += ordinal
                metadata[ordinal] = BulkCopyColumnMetadata(
                    rs.getString("COLUMN_NAME"),
                    rs.getInt("DATA_TYPE"),
                    rs.getInt("COLUMN_SIZE"),
                    rs.getInt("DECIMAL_DIGITS")
                )
            }
        }
    }

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
        return Array(row.size) { row[it] }
    }

    override fun next(): Boolean = iterator.hasNext()
}
