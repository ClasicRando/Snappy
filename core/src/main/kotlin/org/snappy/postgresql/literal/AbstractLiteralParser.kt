package org.snappy.postgresql.literal

import org.snappy.postgresql.instantFormatter
import org.snappy.postgresql.localDateFormatter
import org.snappy.postgresql.localDateTimeFormatter
import org.snappy.postgresql.localTimeFormatter
import org.snappy.postgresql.offsetTimeFormatter
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime

abstract class AbstractLiteralParser(literal: String) {
    protected val charBuffer = literal.substring(1, literal.length - 1).toMutableList()
    protected var isDone = false

    @PublishedApi
    internal inline fun <T : Any?> tryParseNextBuffer(
        expectedType: String,
        errorInfo: () -> String? = { null },
        action: (String) -> T
    ): T? {
        val value = readNextBuffer() ?: return null
        if (value.isEmpty() || value.lowercase() == "null") {
            return null
        }
        return runCatching {
            action(value)
        }.getOrElse {
            val error = LiteralParseError(expectedType, value, errorInfo())
            error.addSuppressed(it)
            throw error
        }
    }

    @PublishedApi
    internal abstract fun readNextBuffer(): String?

    fun readBoolean(): Boolean? {
        val buffer = readNextBuffer() ?: return null
        return when(buffer) {
            "t" -> true
            "f" -> false
            else -> throw LiteralParseError("Boolean", buffer, "value not t/f")
        }
    }

    fun readShort(): Short? = tryParseNextBuffer("Short") {
        it.toShort()
    }

    fun readInt(): Int? = tryParseNextBuffer("Int") {
        it.toInt()
    }

    fun readLong(): Long? = tryParseNextBuffer("Long") {
        it.toLong()
    }

    fun readFloat(): Float? = tryParseNextBuffer("Float") {
        it.toFloat()
    }

    fun readDouble(): Double? = tryParseNextBuffer("Double") {
        it.toDouble()
    }

    fun readBigDecimal(): BigDecimal? = tryParseNextBuffer("BigDecimal") {
        it.toBigDecimal()
    }

    fun readString(): String? = readNextBuffer()

    fun readLocalDate(): LocalDate? = tryParseNextBuffer("LocalDate") {
        LocalDate.from(localDateFormatter.parse(it))
    }

    fun readLocalTime(): LocalTime? = tryParseNextBuffer("LocalTime") {
        LocalTime.from(localTimeFormatter.parse(it))
    }

    fun readLocalDateTime(): LocalDateTime? = tryParseNextBuffer("LocalDateTime") {
        LocalDateTime.from(localDateTimeFormatter.parse(it))
    }

    fun readOffsetTime(): OffsetTime? = tryParseNextBuffer("OffsetTime") {
        OffsetTime.from(offsetTimeFormatter.parse(it))
    }

    fun readOffsetDateTime(): OffsetDateTime? = tryParseNextBuffer("OffsetDateTime") {
        OffsetDateTime.from(instantFormatter.parse(it))
    }

    fun readInstant(): Instant? = tryParseNextBuffer("Instant") {
        Instant.from(instantFormatter.parse(it))
    }

    inline fun <reified T : Enum<T>> readEnum(): T? = tryParseNextBuffer("Enum") {
        enumValues<T>().first { label -> label.name.lowercase() == it.lowercase() }
    }
}
