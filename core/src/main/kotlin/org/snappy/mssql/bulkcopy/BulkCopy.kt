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

/**
 * Bulk copy a [ResultSet] to a [destinationTable]. Creates a new instance of [SQLServerBulkCopy]
 * and calls [SQLServerBulkCopy.writeToServer] with the [ResultSet].
 */
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

/**
 * Bulk copy a [RowSet] to a [destinationTable]. Creates a new instance of [SQLServerBulkCopy] and
 * calls [SQLServerBulkCopy.writeToServer] with the [RowSet].
 */
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

/**
 * Bulk copy a [ISQLServerBulkData] to a [destinationTable]. Creates a new instance of
 * [SQLServerBulkCopy] and calls [SQLServerBulkCopy.writeToServer] with the [ISQLServerBulkData].
 */
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

/**
 * Bulk copy a csv file specified at [sourceFile] to a [destinationTable]. Creates a new instance of
 * [SQLServerBulkCopy] and [SQLServerBulkCSVFileRecord] (with the various options passed included),
 * calling [SQLServerBulkCopy.writeToServer] with the [SQLServerBulkCSVFileRecord]. The structure of
 * the data within the csv file must match the [destinationTable] and must be implicitly convertible
 * to the required column type.
 */
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

/**
 * Bulk copy a csv file provided as an [InputStream] to a [destinationTable]. Creates a new instance
 * of [SQLServerBulkCopy] and [SQLServerBulkCSVFileRecord] (with the various options passed
 * included), calling [SQLServerBulkCopy.writeToServer] with the [SQLServerBulkCSVFileRecord]. The
 * structure of the data within the csv file must match the [destinationTable] and must be
 * implicitly convertible to the required column type.
 */
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

/**
 * Bulk copy a [Sequence] of [ToObjectRow] provided to a [destinationTable]. Creates a new instance
 * of [SQLServerBulkCopy], calling [SQLServerBulkCopy.writeToServer] with the [sequence] wrapped as
 * an [ISQLServerBulkData]. The structure of the data within the [Sequence] must match the
 * [destinationTable] and must be implicitly convertible to the required column type.
 */
fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequence(
    destinationTable: String,
    sequence: Sequence<R>,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
) {
    val sourceData = SequenceBulkCopy(
        sequence,
        fetchMetadata(destinationTable),
    )
    bulkCopy(destinationTable, sourceData, bulkCopyOptions)
}

/**
 * Bulk copy a [Sequence] of [ToObjectRow] provided to a [destinationTable]. The [builder] is used
 * to create a new instance of [Sequence] for pushing results through the bulk copy. Creates a new
 * instance of [SQLServerBulkCopy], calling [SQLServerBulkCopy.writeToServer] with the [sequence]
 * wrapped as an [ISQLServerBulkData]. The structure of the data within the [Sequence] must match
 * the [destinationTable] and must be implicitly convertible to the required column type.
 */
inline fun <R : ToObjectRow> ISQLServerConnection.bulkCopySequence(
    destinationTable: String,
    bulkCopyOptions: SQLServerBulkCopyOptions = SQLServerBulkCopyOptions(),
    crossinline builder: suspend SequenceScope<R>.() -> Unit,
) {
    this.bulkCopySequence(destinationTable, sequence { builder() }, bulkCopyOptions)
}
