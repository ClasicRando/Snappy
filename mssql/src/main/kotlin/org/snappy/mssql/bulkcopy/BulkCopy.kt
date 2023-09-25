package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy
import org.snappy.copy.ToObjectRow
import java.io.InputStream
import java.sql.ResultSet
import javax.sql.RowSet

fun ISQLServerConnection.bulkCopy(
    destinationTable: String,
    sourceData: ResultSet,
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    require(!sourceData.isClosed) { "Cannot bulk copy from a closed result set" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
    }.use {
        it.writeToServer(sourceData)
    }
}

fun ISQLServerConnection.bulkCopy(
    destinationTable: String,
    sourceData: RowSet,
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    require(!sourceData.isClosed) { "Cannot bulk copy from a closed row set" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
    }.use {
        it.writeToServer(sourceData)
    }
}

fun ISQLServerConnection.bulkCopy(
    destinationTable: String,
    sourceData: ISQLServerBulkData,
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
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
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
    }.use {
        val sourceData = SQLServerBulkCSVFileRecord(
            sourceFile,
            encoding,
            delimiter.toString(),
            hasHeader,
        )
        it.writeToServer(sourceData)
    }
}

fun ISQLServerConnection.bulkCopyCsvFile(
    destinationTable: String,
    sourceFile: InputStream,
    encoding: String? = null,
    delimiter: Char = ',',
    hasHeader: Boolean = true,
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
    }.use {
        val sourceData = SQLServerBulkCSVFileRecord(
            sourceFile,
            encoding,
            delimiter.toString(),
            hasHeader,
        )
        it.writeToServer(sourceData)
    }
}

fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequence(
    destinationTable: String,
    sequence: Sequence<R>,
) {
    require(!isClosed) { "Cannot bulk copy on a closed connection" }
    SQLServerBulkCopy(this).apply {
        destinationTableName = destinationTable
    }.use {
        val sourceData = SequenceBulkData(
            destinationTable,
            sequence,
        )
        sourceData.fetchMetadata(this)
        it.writeToServer(sourceData)
    }
}

inline fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequence(
    destinationTable: String,
    crossinline builder: suspend SequenceScope<R>.() -> Unit,
) {
    this.bulkCopySequence(destinationTable, sequence { builder() })
}
