package org.snappy.postgresql.literal

import org.postgresql.util.PGobject
import org.snappy.SnappyMapper
import org.snappy.postgresql.type.PgObjectDecoder
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import kotlin.reflect.KClass
import kotlin.reflect.cast
import kotlin.reflect.full.createType

class PgArrayLiteralParser<T : Any> @PublishedApi internal constructor(
    literal: String,
    private val elementClass: KClass<T>
) : AbstractLiteralParser(literal) {
    private var done = false
    override fun readNextBuffer(): String? {
        if (done) {
            throw ExhaustedBuffer()
        }
        var inQuotes = false
        var inEscape = false
        val builder = StringBuilder()
        while (charBuffer.isNotEmpty()) {
            val char = charBuffer.removeFirst()
            when {
                inEscape -> {
                    builder.append(char)
                    inEscape = false
                }
                char == '"' -> inQuotes = !inQuotes
                char == '\\' -> inEscape = true
                char == DELIMITER && !inQuotes -> break
                else -> builder.append(char)
            }
        }
        done = charBuffer.isEmpty()
        return builder.toString().takeIf { it.isNotEmpty() && it != "NULL" }
    }

    inline fun <reified A: T> parseToArray(): Array<A?> {
        val list = parseToList()
        return Array(list.size) {
            list[it] as A?
        }
    }

    fun parseToList(): List<T?> = buildList {
        while (!done) {
            when (elementClass) {
                Boolean::class -> add(elementClass.cast(readBoolean()))
                Short::class -> add(elementClass.cast(readShort()))
                Int::class -> add(elementClass.cast(readInt()))
                Long::class -> add(elementClass.cast(readLong()))
                Float::class -> add(elementClass.cast(readFloat()))
                Double::class -> add(elementClass.cast(readDouble()))
                BigDecimal::class -> add(elementClass.cast(readBigDecimal()))
                String::class -> add(elementClass.cast(readString()))
                LocalDate::class -> add(elementClass.cast(readLocalDate()))
                LocalTime::class -> add(elementClass.cast(readLocalTime()))
                LocalDateTime::class -> add(elementClass.cast(readLocalDateTime()))
                OffsetTime::class -> add(elementClass.cast(readOffsetTime()))
                OffsetDateTime::class -> add(elementClass.cast(readOffsetDateTime()))
                Instant::class -> add(elementClass.cast(readInstant()))
                else -> {
                    val type = elementClass.createType(nullable = false)
                    val decoder = SnappyMapper.decoderCache.getOrNull(type) as? PgObjectDecoder
                        ?: throw MissingParseType(elementClass)
                    val element = tryParseNextBuffer(elementClass.simpleName ?: "") {
                        decoder.decodePgObject(PGobject().apply { value = it })
                    }
                    add(elementClass.cast(element))
                }
            }
        }
    }

    companion object {
        private const val DELIMITER = ','
    }
}
