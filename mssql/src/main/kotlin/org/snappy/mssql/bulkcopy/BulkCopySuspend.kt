package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData
import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.copy.ToObjectRow
import java.io.InputStream
import java.sql.ResultSet
import javax.sql.RowSet

/**
 * Bulk copy a [ResultSet] to a [destinationTable]. Creates a new instance of [SQLServerBulkCopy]
 * and calls [SQLServerBulkCopy.writeToServer] with the [ResultSet]. Operation is suspended against
 * [Dispatchers.IO].
 */
suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: ResultSet,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData, bulkCopyOptions)
}

/**
 * Bulk copy a [RowSet] to a [destinationTable]. Creates a new instance of [SQLServerBulkCopy] and
 * calls [SQLServerBulkCopy.writeToServer] with the [RowSet]. Operation is suspended against
 * [Dispatchers.IO].
 */
suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: RowSet,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData, bulkCopyOptions)
}

/**
 * Bulk copy a [ISQLServerBulkData] to a [destinationTable]. Creates a new instance of
 * [SQLServerBulkCopy] and calls [SQLServerBulkCopy.writeToServer] with the [ISQLServerBulkData].
 * Operation is suspended against [Dispatchers.IO].
 */
suspend fun ISQLServerConnection.bulkCopySuspend(
    destinationTable: String,
    sourceData: ISQLServerBulkData,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopy(destinationTable, sourceData, bulkCopyOptions)
}

/**
 * Bulk copy a csv file specified at [sourceFile] to a [destinationTable]. Creates a new instance of
 * [SQLServerBulkCopy] and [SQLServerBulkCSVFileRecord] (with the various options passed included),
 * calling [SQLServerBulkCopy.writeToServer] with the [SQLServerBulkCSVFileRecord]. The structure of
 * the data within the csv file must match the [destinationTable] and must be implicitly convertible
 * to the required column type. Operation is suspended against [Dispatchers.IO].
 */
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

/**
 * Bulk copy a csv file provided as an [InputStream] to a [destinationTable]. Creates a new instance
 * of [SQLServerBulkCopy] and [SQLServerBulkCSVFileRecord] (with the various options passed
 * included), calling [SQLServerBulkCopy.writeToServer] with the [SQLServerBulkCSVFileRecord]. The
 * structure of the data within the csv file must match the [destinationTable] and must be
 * implicitly convertible to the required column type. Operation is suspended against
 * [Dispatchers.IO].
 */
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

/**
 * Bulk copy a [Sequence] of [ToObjectRow] provided to a [destinationTable]. Creates a new instance
 * of [SQLServerBulkCopy], calling [SQLServerBulkCopy.writeToServer] with the [sequence] wrapped as
 * an [ISQLServerBulkData]. The structure of the data within the [Sequence] must match the
 * [destinationTable] and must be implicitly convertible to the required column type. Operation is
 * suspended against [Dispatchers.IO].
 */
suspend fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequenceSuspend(
    destinationTable: String,
    sequence: Sequence<R>,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) = withContext(Dispatchers.IO) {
    bulkCopySequence(destinationTable, sequence, bulkCopyOptions)
}

/**
 * Bulk copy a [Sequence] of [ToObjectRow] provided to a [destinationTable]. The [builder] is used
 * to create a new instance of [Sequence] for pushing results through the bulk copy. Creates a new
 * instance of [SQLServerBulkCopy], calling [SQLServerBulkCopy.writeToServer] with the [sequence]
 * wrapped as an [ISQLServerBulkData]. The structure of the data within the [Sequence] must match
 * the [destinationTable] and must be implicitly convertible to the required column type. Operation
 * is suspended against [Dispatchers.IO].
 */
suspend inline fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequenceSuspend(
    destinationTable: String,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
    crossinline builder: suspend SequenceScope<R>.() -> Unit,
) = bulkCopySequenceSuspend<R>(destinationTable, sequence { builder() }, bulkCopyOptions)
