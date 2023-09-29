package org.snappy.postgresql.literal

import org.snappy.postgresql.instantFormatter
import org.snappy.postgresql.localDateFormatter
import org.snappy.postgresql.localDateTimeFormatter
import org.snappy.postgresql.localTimeFormatter
import org.snappy.postgresql.offsetTimeFormatter
import org.snappy.postgresql.type.ToPgObject
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime

class PgCompositeLiteralBuilder {
    private val stringBuilder = StringBuilder().apply {
        append('(')
    }

    /** Returns a new string with the required control characters for composites escaped */
    private fun replaceInComposite(value: String?): String? {
        return value?.replace("\\", "\\\\")?.replace("\"", "\"\"")
    }

    private fun prependCommaIfNeeded() {
        if (stringBuilder.length == 1) {
            return
        }
        stringBuilder.append(',')
    }

    fun <C : ToPgObject> appendComposite(composite: C?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        composite?.let { comp ->
            val pgObject = comp.toPgObject()
            stringBuilder.apply {
                append('"')
                append(replaceInComposite(pgObject.value))
                append('"')
            }
        }
        return this
    }

    fun <T> appendIterable(iterable: Iterable<T>?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        iterable?.let { iter ->
            stringBuilder.apply {
                append('"')
                val arrayString = iter.toPgArrayLiteral()
                append(replaceInComposite(arrayString))
                append('"')
            }
        }
        return this
    }

    fun <T> appendArray(array: Array<T>?): PgCompositeLiteralBuilder {
        return appendIterable(array?.asIterable())
    }

    fun appendBoolean(bool: Boolean?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        bool?.let { stringBuilder.append(if (it) 't' else 'f') }
        return this
    }

    fun appendByte(byte: Byte?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        stringBuilder.append(byte)
        return this
    }

    fun appendShort(short: Short?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        stringBuilder.append(short)
        return this
    }

    fun appendInt(int: Int?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        stringBuilder.append(int)
        return this
    }

    fun appendLong(long: Long?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        stringBuilder.append(long)
        return this
    }

    fun appendFloat(float: Float?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        stringBuilder.append(float)
        return this
    }

    fun appendDouble(double: Double?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        stringBuilder.append(double)
        return this
    }

    fun appendString(string: String?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        if (string == null) {
            return this
        }
        stringBuilder.apply {
            append('"')
            append(string.replace("\"", "\"\""))
            append('"')
        }
        return this
    }

    fun appendBigDecimal(bigDecimal: BigDecimal?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        stringBuilder.append(bigDecimal)
        return this
    }

    fun appendDate(date: Date?): PgCompositeLiteralBuilder {
        return appendLocalDate(date?.toLocalDate())
    }

    fun appendLocalDate(localDate: LocalDate?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        localDate?.let { stringBuilder.append(localDateFormatter.format(it)) }
        return this
    }

    fun appendLocalDateTime(localDateTime: LocalDateTime?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        localDateTime?.let {
            stringBuilder.apply {
                append('"')
                append(localDateTimeFormatter.format(it))
                append('"')
            }
        }
        return this
    }

    fun appendTimestamp(timestamp: Timestamp?): PgCompositeLiteralBuilder {
        return appendInstant(timestamp?.toInstant())
    }

    fun appendOffsetDateTime(offsetDateTime: OffsetDateTime?): PgCompositeLiteralBuilder {
        return appendInstant(offsetDateTime?.toInstant())
    }

    fun appendInstant(instant: Instant?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        instant?.let {
            stringBuilder.apply {
                append('"')
                append(instantFormatter.format(it))
                append('"')
            }
        }
        return this
    }

    fun appendTime(time: Time?): PgCompositeLiteralBuilder {
        return appendLocalTime(time?.toLocalTime())
    }

    fun appendLocalTime(localTime: LocalTime?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        localTime?.let { stringBuilder.append(localTimeFormatter.format(it)) }
        return this
    }

    fun appendOffsetTime(offsetTime: OffsetTime?): PgCompositeLiteralBuilder {
        prependCommaIfNeeded()
        offsetTime?.let { stringBuilder.append(offsetTimeFormatter.format(it)) }
        return this
    }

    override fun toString(): String {
        return stringBuilder.run {
            append(')')
            toString()
        }
    }
}
