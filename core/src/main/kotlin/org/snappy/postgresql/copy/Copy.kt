package org.snappy.postgresql.copy

import org.postgresql.PGConnection
import org.snappy.copy.ToObjectRow
import org.snappy.logging.logger
import java.io.InputStream
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Converts an array of String values to a ByteArray that reflects a CSV record. Used to pipe output
 * to COPY command
 */
internal fun recordToCsvBytes(record: Sequence<String>): ByteArray {
    return record.joinToString(separator = "\",\"", prefix = "\"", postfix = "\"\n") { value ->
        value.replace("\"", "\"\"")
    }.toByteArray()
}

/**
 * Obtain the Postgresql COPY command for the specified [tableName] through a stream with various
 * format options. The byte stream will always be CSV file like with a specified [delimiter] and a
 * possible [header] line. There is also an option for non-qualified files where the QUOTE and
 * ESCAPE options are not set.
 */
@PublishedApi
internal fun getCopyCommand(
    tableName: String,
    header: Boolean,
    columnNames: List<String>,
    delimiter: Char,
    qualified: Boolean = true,
): String {
    return """
        COPY ${tableName.lowercase()} (${columnNames.joinToString()})
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER '$delimiter',
            HEADER $header${if (qualified) ", QUOTE '\"', ESCAPE '\"'" else ""}
        )
    """.trimIndent()
}

/**
 * Accepts a nullable object and formats the value to string. Most of the formatting is for
 * Date-like types that all get converted to ISO formats. NOTE: [Instant] values are set to
 * [ZoneOffset.UTC] before formatting so the values copied to the table will be of the same zone.
 */
@PublishedApi
internal fun formatObject(value: Any?): String {
    return when(value) {
        null -> ""
        is Boolean -> if (value) "TRUE" else "FALSE"
        is String -> value
        is BigDecimal -> value.toPlainString()
        is ByteArray -> value.decodeToString()
        is Instant -> value.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)
        is OffsetDateTime -> value.format(DateTimeFormatter.ISO_DATE_TIME)
        is OffsetTime -> value.format(DateTimeFormatter.ISO_TIME)
        is LocalDateTime -> value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        is LocalDate -> value.format(DateTimeFormatter.ISO_LOCAL_DATE)
        is LocalTime -> value.format(DateTimeFormatter.ISO_LOCAL_TIME)
        else -> value.toString()
    }
}

/** Logger for blocking copy operations */
private val log by logger()

/** Copy an [inputStream] through the connection using the [copyCommand] specified */
fun PGConnection.copyIn(copyCommand: String, inputStream: InputStream): Long {
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
    log.atTrace {
        message = "Completed copying $result records from InputStream"
        payload = mapOf("copy command" to copyCommand)
    }
    return result
}

/**
 * Copy an [inputStream] through the connection, constructing a COPY command using the parameters
 * provided
 */
fun PGConnection.copyIn(
    inputStream: InputStream,
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyIn(copyCommand, inputStream)
}

/**
 * Base logic when attempting to perform a COPY with a [Sequence] of CSV [records]. Fetches a
 * [org.postgresql.copy.CopyIn] client, iterating over the [records] and converting each record into
 * a [ByteArray] for writing to the [org.postgresql.copy.CopyIn] client. After all records have been
 * passed to the server, [org.postgresql.copy.CopyIn.endCopy] is called to get the number of records
 * wrote to the server.
 *
 * If an exception is thrown during the copy operation, [org.postgresql.copy.CopyIn.cancelCopy] is
 * called to abort the operation and no records are saved.
 */
@PublishedApi
internal fun PGConnection.copyInInternal(
    copyCommand: String,
    records: Sequence<Sequence<String>>,
): Long {
    val copyStream = copyAPI.copyIn(copyCommand)
    val result = try {
        for (record in records) {
            val bytes = recordToCsvBytes(record)
            copyStream.writeToCopy(bytes, 0, bytes.size)
        }
        copyStream.endCopy()
    } catch (ex: Throwable) {
        log.atError {
            message = "Error during copy"
            cause = ex
            payload = mapOf("copy command" to copyCommand)
        }
        try { copyStream.cancelCopy() } catch (_: Exception) {}
        throw ex
    }
    log.atTrace {
        message = "Completed copying $result records from sequence of items"
        payload = mapOf("copy command" to copyCommand)
    }
    return result
}

/** Execute the [copyCommand] provided, writing the [records] to the server */
fun <T : ToCsvRow> PGConnection.copyInCsv(copyCommand: String, records: Sequence<T>): Long {
    return copyInInternal(copyCommand, records.map { it.toCsvRow().asSequence() })
}

/**
 * Execute a COPY command, writing the [records] to the server. The other parameters specified are
 * used to construct the COPY command
 */
fun <T : ToCsvRow> PGConnection.copyInCsv(
    records: Sequence<T>,
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInCsv(copyCommand, records)
}

/** Execute the [copyCommand] provided, writing the data yielded from [records] to the server */
inline fun <T : ToCsvRow> PGConnection.copyInCsv(
    copyCommand: String,
    crossinline records: suspend SequenceScope<T>.() -> Unit,
): Long {
    return copyInCsv(copyCommand, sequence { records() })
}

/**
 * Execute a COPY command, writing data yielded from [records] to the server. The other parameters
 * specified are used to construct the COPY command
 */
inline fun <T : ToCsvRow> PGConnection.copyInCsv(
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
    crossinline records: suspend SequenceScope<T>.() -> Unit,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInCsv(copyCommand, records)
}

/** Execute the [copyCommand] provided, writing the [records] to the server */
fun <T : ToObjectRow> PGConnection.copyInRow(copyCommand: String, records: Sequence<T>): Long {
    return copyInInternal(
        copyCommand,
        records.map { record ->
            record.toObjectRow().asSequence().map { obj -> formatObject(obj) }
        },
    )
}

/**
 * Execute a COPY command, writing the [records] to the server. The other parameters specified are
 * used to construct the COPY command
 */
fun <T : ToObjectRow> PGConnection.copyInRow(
    records: Sequence<T>,
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInRow(copyCommand, records)
}

/** Execute the [copyCommand] provided, writing the data yielded from [records] to the server */
inline fun <T : ToObjectRow> PGConnection.copyInRow(
    copyCommand: String,
    crossinline records: suspend SequenceScope<T>.() -> Unit,
): Long {
    return copyInRow(copyCommand, sequence { records() })
}

/**
 * Execute a COPY command, writing data yielded from [records] to the server. The other parameters
 * specified are used to construct the COPY command
 */
inline fun <T : ToObjectRow> PGConnection.copyInRow(
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
    crossinline records: suspend SequenceScope<T>.() -> Unit
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInRow(copyCommand, records)
}
