package org.snappy.postgresql.copy

import org.postgresql.PGConnection
import java.io.InputStream
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Converts an array of String values to a ByteArray that reflects a CSV record. Used to pipe output
 * to COPY command
 */
internal fun recordToCsvBytes(record: Iterable<String>): ByteArray {
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
 * Date-like types that all get converted to ISO formats. NOTE: [Timestamp], [Date] and [Instant]
 * values are set to [ZoneOffset.UTC] before formatting so the values copied to the table will be of
 * the same zone.
 */
internal fun formatObject(value: Any?): String {
    return when(value) {
        null -> ""
        is Boolean -> if (value) "TRUE" else "FALSE"
        is String -> value
        is BigDecimal -> value.toPlainString()
        is ByteArray -> value.decodeToString()
        is Timestamp -> value.toInstant().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)
        is Instant -> value.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)
        is LocalDateTime -> value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        is LocalDate -> value.format(DateTimeFormatter.ISO_LOCAL_DATE)
        is LocalTime -> value.format(DateTimeFormatter.ISO_LOCAL_TIME)
        is Date -> value.toInstant().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)
        is Time -> value.toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME)
        else -> value.toString()
    }
}

fun PGConnection.copyIn(copyCommand: String, inputStream: InputStream): Long {
    return copyAPI.copyIn(copyCommand, inputStream)
}

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

internal fun PGConnection.copyInInternal(
    copyCommand: String,
    records: Sequence<Iterable<String>>,
): Long {
    val copyStream = copyAPI.copyIn(copyCommand)
    return try {
        for (record in records) {
            val bytes = recordToCsvBytes(record)
            copyStream.writeToCopy(bytes, 0, bytes.size)
        }
        copyStream.endCopy()
    } catch (ex: Exception) {
        try { copyStream.cancelCopy() } catch (_: Exception) {}
        throw ex
    }
}

fun <T : IntoCsvRow> PGConnection.copyInCsv(copyCommand: String, records: Sequence<T>): Long {
    return copyInInternal(copyCommand, records.map { it.intoCsvRow() })
}

fun <T : IntoCsvRow> PGConnection.copyInCsv(
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

fun <T : IntoCsvRow> PGConnection.copyInCsv(
    copyCommand: String,
    records: suspend SequenceScope<T>.() -> Unit,
): Long {
    return copyInInternal(copyCommand, sequence { records() }.map { it.intoCsvRow() })
}

fun <T : IntoCsvRow> PGConnection.copyInCsv(
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
    records: suspend SequenceScope<T>.() -> Unit,
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInCsv(copyCommand, records)
}

fun <T : IntoObjectRow> PGConnection.copyInRow(copyCommand: String, records: Sequence<T>): Long {
    return copyInInternal(
        copyCommand,
        records.map { record -> record.intoObjectRow().map { obj -> formatObject(obj) } },
    )
}

fun <T : IntoObjectRow> PGConnection.copyInRow(
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

fun <T : IntoObjectRow> PGConnection.copyInRow(
    copyCommand: String,
    records: suspend SequenceScope<T>.() -> Unit,
): Long {
    return copyInInternal(
        copyCommand,
        sequence {
            records()
        }.map { record ->
            record.intoObjectRow().map { obj -> formatObject(obj) }
        },
    )
}

fun <T : IntoObjectRow> PGConnection.copyInRow(
    tableName: String,
    header: Boolean,
    columNames: List<String>,
    delimiter: Char = ',',
    qualified: Boolean = true,
    records: suspend SequenceScope<T>.() -> Unit
): Long {
    val copyCommand = getCopyCommand(tableName, header, columNames, delimiter, qualified)
    return copyInRow(copyCommand, records)
}
