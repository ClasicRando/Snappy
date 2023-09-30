package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerConnection

/**
 * Fetches the column details about a destination table to provide the data required for a bulk copy
 * operation
 */
internal fun ISQLServerConnection.fetchMetadata(
    destinationTable: String,
): Map<Int, BulkCopyColumnMetadata> = buildMap {
    this@fetchMetadata.metaData.getColumns(
        null,
        null,
        destinationTable,
        null,
    ).use { rs ->
        while (rs.next()) {
            val ordinal = rs.getInt("ORDINAL_POSITION")
            this[ordinal] = BulkCopyColumnMetadata(
                rs.getString("COLUMN_NAME"),
                rs.getInt("DATA_TYPE"),
                rs.getInt("COLUMN_SIZE"),
                rs.getInt("DECIMAL_DIGITS"),
            )
        }
    }
}
