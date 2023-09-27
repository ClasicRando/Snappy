package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions
import org.snappy.copy.ToObjectRow
import java.io.InputStream
import java.sql.ResultSet
import javax.sql.RowSet

fun ISQLServerConnection.bulkCopy(
    destinationTable: String,
    sourceData: ResultSet,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    require(!sourceData.isClosed) { "Cannot bulk copy from a closed result set" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
        this.bulkCopyOptions = bulkCopyOptions
    }.use {
        it.writeToServer(sourceData)
    }
}

fun ISQLServerConnection.bulkCopy(
    destinationTable: String,
    sourceData: RowSet,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    require(!sourceData.isClosed) { "Cannot bulk copy from a closed row set" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
        this.bulkCopyOptions = bulkCopyOptions
    }.use {
        it.writeToServer(sourceData)
    }
}

fun ISQLServerConnection.bulkCopy(
    destinationTable: String,
    sourceData: ISQLServerBulkData,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
        this.bulkCopyOptions = bulkCopyOptions
    }.use {
        it.writeToServer(sourceData)
    }
}

fun ISQLServerConnection.bulkCopyCsvFile(
    destinationTable: String,
    sourceFile: String,
    encoding: String? = null,
    delimiter: Char = ',',
    hasHeader: Boolean = true,
    isQualified: Boolean = true,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
        this.bulkCopyOptions = bulkCopyOptions
    }.use {
        val sourceData = SQLServerBulkCSVFileRecord(
            sourceFile,
            encoding,
            delimiter.toString(),
            hasHeader,
        )
        sourceData.isEscapeColumnDelimitersCSV = isQualified
        for ((ordinal, type) in fetchMetadata(destinationTable)) {
            sourceData.addColumnMetadata(
                ordinal,
                null,
                type.type,
                type.precision ?: 0,
                type.scale ?: 0,
            )
        }
        it.writeToServer(sourceData)
    }
}

fun ISQLServerConnection.bulkCopyCsvFile(
    destinationTable: String,
    sourceFile: InputStream,
    encoding: String? = null,
    delimiter: Char = ',',
    hasHeader: Boolean = true,
    isQualified: Boolean = true,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
        this.bulkCopyOptions = bulkCopyOptions
    }.use {
        val sourceData = SQLServerBulkCSVFileRecord(
            sourceFile,
            encoding,
            delimiter.toString(),
            hasHeader,
        )
        sourceData.isEscapeColumnDelimitersCSV = isQualified
        for ((ordinal, type) in fetchMetadata(destinationTable)) {
            sourceData.addColumnMetadata(
                ordinal,
                null,
                type.type,
                type.precision ?: 0,
                type.scale ?: 0,
            )
        }
        it.writeToServer(sourceData)
    }
}

fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequence(
    destinationTable: String,
    sequence: Sequence<R>,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
        this.bulkCopyOptions = bulkCopyOptions
    }.use {
        val sourceData = SequenceBulkCopy(
            sequence,
            fetchMetadata(destinationTable),
        )
        it.writeToServer(sourceData)
    }
}

inline fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequence(
    destinationTable: String,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
    crossinline builder: suspend SequenceScope<R>.() -> Unit,
) {
    this.bulkCopySequence(destinationTable, sequence { builder() }, bulkCopyOptions)
}
