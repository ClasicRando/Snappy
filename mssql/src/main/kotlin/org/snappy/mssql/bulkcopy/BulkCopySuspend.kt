package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.copy.ToObjectRow
import java.io.InputStream
import java.sql.ResultSet
import javax.sql.RowSet

suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: ResultSet,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData, bulkCopyOptions)
}

suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: RowSet,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData, bulkCopyOptions)
}

suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: ISQLServerBulkData,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData, bulkCopyOptions)
}

suspend fun ISQLServerConnection.bulkCopyCsvFileSuspend(
    destinationTable: String,
    sourceFile: String,
    encoding: String? = null,
    delimiter: Char = ',',
    hasHeader: Boolean = true,
    isQualified: Boolean = true,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopyCsvFile(
        destinationTable,
        sourceFile,
        encoding,
        delimiter,
        hasHeader,
        isQualified,
        bulkCopyOptions,
    )
}

suspend fun ISQLServerConnection.bulkCopyCsvFileSuspend(
    destinationTable: String,
    sourceFile: InputStream,
    encoding: String? = null,
    delimiter: Char = ',',
    hasHeader: Boolean = true,
    isQualified: Boolean = true,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopyCsvFile(
        destinationTable,
        sourceFile,
        encoding,
        delimiter,
        hasHeader,
        isQualified,
        bulkCopyOptions,
    )
}

suspend fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequenceSuspend(
    destinationTable: String,
    sequence: Sequence<R>,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopySequence(destinationTable, sequence, bulkCopyOptions)
}

suspend inline fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequenceSuspend(
    destinationTable: String,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
    crossinline builder: suspend SequenceScope<R>.() -> Unit,
) = bulkCopySequenceSuspend<R>(destinationTable, sequence { builder() }, bulkCopyOptions)
