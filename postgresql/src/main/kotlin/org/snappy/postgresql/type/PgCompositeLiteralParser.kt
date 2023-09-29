package org.snappy.postgresql.type

import org.postgresql.util.PGobject
import org.snappy.SnappyMapper
import org.snappy.postgresql.array.PgArrayLiteralParser
import org.snappy.postgresql.literal.AbstractLiteralParser
import org.snappy.postgresql.literal.ExhaustedBuffer
import kotlin.reflect.KClass
import kotlin.reflect.KType

fun <T : Any> parseComposite(pgObject: PGobject, parsing: PgCompositeLiteralParser.() -> T): T? {
    return pgObject.value?.let {
        val parser = PgCompositeLiteralParser(it)
        parser.parsing()
    }
}

class PgCompositeLiteralParser internal constructor(value: String) : AbstractLiteralParser(value) {
    override fun readNextBuffer(): String? {
        if (charBuffer.isEmpty()) {
            throw ExhaustedBuffer()
        }
        var quoted = false
        var inQuotes = false
        var inEscape = false
        var previousChar = '\u0000'
        val builder = StringBuilder()
        while (charBuffer.isNotEmpty()) {
            val char = charBuffer.removeFirst()
            when {
                inEscape -> {
                    builder.append(char)
                    inEscape = false
                }
                char == '"' && inQuotes -> inQuotes = false
                char == '"' -> {
                    inQuotes = true
                    quoted = true
                    if (previousChar == '"') {
                        builder.append(char)
                    }
                }
                char == '\\' && !inEscape -> inEscape = true
                char == ',' && !inQuotes -> break
                else -> builder.append(char)
            }
            previousChar = char
        }
        return builder.takeIf { it.isNotEmpty() || quoted }?.toString()
    }

    fun <C : Any> readComposite(decoder: PgObjectDecoder<C>): C? {
        return tryParseNextBuffer("Composite") {
            decoder.decodePgObject(PGobject().apply { value = it })
        }
    }

    inline fun <reified C : Any> readComposite(): C? {
        val decoder = SnappyMapper.decoderCache.getOrNull<C>()
            ?: error("Could not find decoder for composite type '${C::class.qualifiedName}'")
        return readComposite(decoder as PgObjectDecoder<C>)
    }

    inline fun <reified T : Any> readArray(kClass: KClass<T>): Array<T?>? {
        return tryParseNextBuffer("Array ${kClass.qualifiedName}") {
            PgArrayLiteralParser(it, kClass).parseToArray()
        }
    }

    inline fun <reified T : Any> readArray(): Array<T?>? {
        return readArray(T::class)
    }

    fun <T : Any> readList(kClass: KClass<T>): List<T?>? {
        return tryParseNextBuffer("Array ${kClass.qualifiedName}") {
            PgArrayLiteralParser(it, kClass).parseToList()
        }
    }

    inline fun <reified T : Any> readList(): List<T?>? {
        return readList(T::class)
    }
}
