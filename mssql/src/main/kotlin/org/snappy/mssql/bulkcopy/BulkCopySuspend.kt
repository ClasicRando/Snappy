package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.copy.ToObjectRow
import java.io.InputStream
import java.sql.ResultSet
import javax.sql.RowSet

suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: ResultSet,
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData)
}

suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: RowSet,
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData)
}

suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: ISQLServerBulkData,
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData)
}

suspend fun ISQLServerConnection.bulkCopyCsvFileSuspend(
    destinationTable: String,
    sourceFile: String,
    encoding: String? = null,
    delimiter: Char = ',',
    hasHeader: Boolean = true,
) = withContext(Dispatchers.IO) {
    bulkCopyCsvFile(destinationTable, sourceFile, encoding, delimiter, hasHeader)
}

suspend fun ISQLServerConnection.bulkCopyCsvFileSuspend(
    destinationTable: String,
    sourceFile: InputStream,
    encoding: String? = null,
    delimiter: Char = ',',
    hasHeader: Boolean = true,
) = withContext(Dispatchers.IO) {
    bulkCopyCsvFile(destinationTable, sourceFile, encoding, delimiter, hasHeader)
}

suspend fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequenceSuspend(
    destinationTable: String,
    sequence: Sequence<R>,
) = withContext(Dispatchers.IO) {
    bulkCopySequence(destinationTable, sequence)
}

suspend inline fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequenceSuspend(
    destinationTable: String,
    crossinline builder: suspend SequenceScope<R>.() -> Unit,
) = bulkCopySequenceSuspend<R>(destinationTable, sequence { builder() })

