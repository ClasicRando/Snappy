package org.snappy.postgresql.literal

import org.snappy.postgresql.instantFormatter
import org.snappy.postgresql.localDateFormatter
import org.snappy.postgresql.localDateTimeFormatter
import org.snappy.postgresql.localTimeFormatter
import org.snappy.postgresql.type.ToPgObject
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

/** Returns a new string with the required control characters for arrays escaped */
private fun replaceInArray(value: String?): String? {
    return value?.replace("\\", "\\\\")?.replace("\"", "\\\"")
}

internal fun <T> Iterable<T>.toPgArrayLiteral(): String {
    val isComposite = this.firstOrNull { it != null }
        ?.let { it is ToPgObject }
        ?: false
    return this.joinToString(
        separator = if (isComposite) "\",\"" else ",",
        prefix = if (isComposite) "{\"" else "{",
        postfix = if (isComposite) "\"}" else "}",
    ) { item ->
        if (item == null) {
            return@joinToString ""
        }
        val element = when (item) {
            is ToPgObject -> item.toPgObject().value ?: ""
            is Time -> localTimeFormatter.format(item.toLocalTime())
            is Date -> localDateFormatter.format(item.toLocalDate())
            is Timestamp -> "\"${instantFormatter.format(item.toInstant())}\""
            is LocalTime -> localTimeFormatter.format(item)
            is LocalDate -> localDateFormatter.format(item)
            is LocalDateTime -> "\"${localDateTimeFormatter.format(item)}\""
            is Instant -> "\"${instantFormatter.format(item)}\""
            else -> item.toString()
        }
        replaceInArray(element) ?: ""
    }
}

@PublishedApi
internal fun <T> Array<T>.toPgArrayLiteral(): String {
    return this.asIterable().toPgArrayLiteral()
}
