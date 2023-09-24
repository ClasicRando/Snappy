package org.snappy.postgresql.copy

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import org.postgresql.PGConnection
import org.snappy.logging.logger
import java.io.InputStream

/** Logger for suspending copy operations */
private val log by logger()

/**
 * Copy an [inputStream] through the connection using the [copyCommand] specified. Operation is
 * suspended against [Dispatchers.IO].
 */
suspend fun PGConnection.copyInSuspend(
    copyCommand: String,
    inputStream: InputStream,
): Long = withContext(Dispatchers.IO) {
    val result = try {
        copyAPI.copyIn(copyCommand, inputStream)
    } catch (ex: Throwable) {
        log.atError {
            message = "Error during copy"
            cause = ex
            payload = mapOf("copy command" to copyCommand)
        }
        throw ex
    }
    log.atInfo {
        message = "Completed copying $result records from InputStream"
        payload = mapOf("copy command" to copyCommand)
    }
    result
}

/**
 * Copy an [inputStream] through the connection, constructing a COPY command using the parameters
 * provided. Operation is suspended against [Dispatchers.IO].
 */
suspend fun PGConnection.copyInSuspend(
    inputStream: InputStream,
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInSuspend(copyCommand, inputStream)
}

/**
 * Base logic when attempting to perform a COPY with a [Sequence] of CSV [records]. Fetches a
 * [org.postgresql.copy.CopyIn] client, collecting the [Flow] of [records] and converting each
 * record into a [ByteArray] for writing to the [org.postgresql.copy.CopyIn] client. After all
 * records have been passed to the server, [org.postgresql.copy.CopyIn.endCopy] is called to get the
 * number of records wrote to the server.
 *
 * If an exception is thrown during the copy operation, [org.postgresql.copy.CopyIn.cancelCopy] is
 * called to abort the operation and no records are saved.
 *
 * NOTE: Both the entire methods is suspended against [Dispatchers.IO] and the [Flow] of [records]
 * is set to [Flow.flowOn] [Dispatchers.IO].
 */
@PublishedApi
internal suspend fun PGConnection.copyInSuspendInternal(
    copyCommand: String,
    records: Flow<Iterable<String>>,
): Long = withContext(Dispatchers.IO) {
    val copyStream = copyAPI.copyIn(copyCommand)
    val result = try {
        records.flowOn(Dispatchers.IO).collect { record ->
            val bytes = recordToCsvBytes(record)
            copyStream.writeToCopy(bytes, 0, bytes.size)
        }
        copyStream.endCopy()
    } catch (ex: Exception) {
        log.atError {
            message = "Error during copy"
            cause = ex
            payload = mapOf("copy command" to copyCommand)
        }
        try { copyStream.cancelCopy() } catch (_: Exception) {}
        throw ex
    }
    log.atInfo {
        message = "Completed copying $result records from sequence of items"
        payload = mapOf("copy command" to copyCommand)
    }
    result
}

/**
 * Execute the [copyCommand] provided, writing the [records] to the server. Operation is suspended
 * against [Dispatchers.IO].
 */
suspend fun <T : ToCsvRow> PGConnection.copyInCsvSuspend(
    copyCommand: String,
    records: Flow<T>,
): Long {
    return copyInSuspendInternal(copyCommand, records.map { it.toCsvRow() })
}

/**
 * Execute a COPY command, writing the [records] to the server. The other parameters specified are
 * used to construct the COPY command. Operation is suspended against [Dispatchers.IO].
 */
suspend fun <T : ToCsvRow> PGConnection.copyInCsvSuspend(
    records: Flow<T>,
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInCsvSuspend(copyCommand, records)
}

/**
 * Execute the [copyCommand] provided, writing the data yielded from [records] to the server.
 * Operation is suspended against [Dispatchers.IO].
 */
suspend inline fun <T : ToCsvRow> PGConnection.copyInCsvSuspend(
    copyCommand: String,
    crossinline records: suspend FlowCollector<T>.() -> Unit,
): Long {
    return copyInSuspendInternal(copyCommand, flow { records() }.map { it.toCsvRow() })
}

/**
 * Execute a COPY command, writing data yielded from [records] to the server. The other parameters
 * specified are used to construct the COPY command. Operation is suspended against
 * [Dispatchers.IO].
 */
suspend inline fun <T : ToCsvRow> PGConnection.copyInCsvSuspend(
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
    crossinline records: suspend FlowCollector<T>.() -> Unit,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInCsvSuspend(copyCommand, records)
}

/**
 * Execute the [copyCommand] provided, writing the [records] to the server. Operation is suspended
 * against [Dispatchers.IO].
 */
suspend fun <T : ToObjectRow> PGConnection.copyInRowSuspend(
    copyCommand: String,
    records: Flow<T>,
): Long {
    return copyInSuspendInternal(
        copyCommand,
        records.map { record -> record.toObjectRow().map { obj -> formatObject(obj) } },
    )
}

/**
 * Execute a COPY command, writing the [records] to the server. The other parameters specified are
 * used to construct the COPY command. Operation is suspended against [Dispatchers.IO].
 */
suspend fun <T : ToObjectRow> PGConnection.copyInRowSuspend(
    records: Flow<T>,
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInRowSuspend(copyCommand, records)
}

/**
 * Execute the [copyCommand] provided, writing the data yielded from [records] to the server.
 * Operation is suspended against [Dispatchers.IO].
 */
suspend inline fun <T : ToObjectRow> PGConnection.copyInRowSuspend(
    copyCommand: String,
    crossinline records: suspend FlowCollector<T>.() -> Unit,
): Long {
    return copyInSuspendInternal(
        copyCommand,
        flow {
            records()
        }.map { record ->
            record.toObjectRow().map { obj -> formatObject(obj) }
        },
    )
}

/**
 * Execute a COPY command, writing data yielded from [records] to the server. The other parameters
 * specified are used to construct the COPY command. Operation is suspended against
 * [Dispatchers.IO].
 */
suspend inline fun <T : ToObjectRow> PGConnection.copyInRowSuspend(
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
    crossinline records: suspend FlowCollector<T>.() -> Unit
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInRowSuspend(copyCommand, records)
}
