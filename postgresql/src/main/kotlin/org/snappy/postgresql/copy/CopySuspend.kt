package org.snappy.postgresql.copy

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import org.postgresql.PGConnection
import java.io.InputStream

suspend fun PGConnection.copyInSuspend(
    copyCommand: String,
    inputStream: InputStream,
): Long = withContext(Dispatchers.IO) {
    copyAPI.copyIn(copyCommand, inputStream)
}

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

internal suspend fun PGConnection.copyInSuspend(
    copyCommand: String,
    records: Flow<Iterable<String>>,
): Long = withContext(Dispatchers.IO) {
    val copyStream = copyAPI.copyIn(copyCommand)
    try {
        records.flowOn(Dispatchers.IO).collect { record ->
            val bytes = recordToCsvBytes(record)
            copyStream.writeToCopy(bytes, 0, bytes.size)
        }
        copyStream.endCopy()
    } catch (ex: Exception) {
        try { copyStream.cancelCopy() } catch (_: Exception) {}
        throw ex
    }
}

suspend fun <T : IntoCsvRow> PGConnection.copyInSuspend(
    copyCommand: String,
    records: Flow<T>,
): Long {
    return copyInSuspend(copyCommand, records.map { it.intoCsvRow() })
}

suspend fun <T : IntoCsvRow> PGConnection.copyInSuspend(
    records: Flow<T>,
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInSuspend(copyCommand, records)
}

suspend fun <T : IntoCsvRow> PGConnection.copyInSuspend(
    copyCommand: String,
    records: suspend FlowCollector<T>.() -> Unit,
): Long {
    return copyInSuspend(copyCommand, flow { records() }.map { it.intoCsvRow() })
}

suspend fun <T : IntoCsvRow> PGConnection.copyInSuspend(
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
    records: suspend FlowCollector<T>.() -> Unit,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInSuspend(copyCommand, records)
}

suspend fun <T : IntoObjectRow> PGConnection.copyInSuspend(
    copyCommand: String,
    records: Flow<T>,
): Long {
    return copyInSuspend(
        copyCommand,
        records.map { record -> record.intoObjectRow().map { obj -> formatObject(obj) } },
    )
}

suspend fun <T : IntoObjectRow> PGConnection.copyInSuspend(
    records: Flow<T>,
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInSuspend(copyCommand, records)
}

suspend fun <T : IntoObjectRow> PGConnection.copyInSuspend(
    copyCommand: String,
    records: suspend FlowCollector<T>.() -> Unit,
): Long {
    return copyInSuspend(
        copyCommand,
        flow {
            records()
        }.map { record ->
            record.intoObjectRow().map { obj -> formatObject(obj) }
        },
    )
}

suspend fun <T : IntoObjectRow> PGConnection.copyInSuspend(
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
    records: suspend FlowCollector<T>.() -> Unit
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInSuspend(copyCommand, records)
}
