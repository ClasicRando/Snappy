@file:Suppress("UNUSED")
package org.snappy.postgresql.array

import org.postgresql.util.PGobject
import org.snappy.SnappyMapper
import org.snappy.decode.Decoder
import org.snappy.decodeError
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant

class BooleanListDecoder: Decoder<List<Boolean>> {
    override fun decode(value: Any?): List<Boolean>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class ByteListDecoder: Decoder<List<Byte>> {
    override fun decode(value: Any?): List<Byte>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class ShortListDecoder: Decoder<List<Short>> {
    override fun decode(value: Any?): List<Short>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class IntListDecoder: Decoder<List<Int>> {
    override fun decode(value: Any?): List<Int>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class LongListDecoder: Decoder<List<Long>> {
    override fun decode(value: Any?): List<Long>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class FloatListDecoder: Decoder<List<Float>> {
    override fun decode(value: Any?): List<Float>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class DoubleListDecoder: Decoder<List<Double>> {
    override fun decode(value: Any?): List<Double>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class InstantListDecoder: Decoder<List<Instant>> {
    override fun decode(value: Any?): List<Instant>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class StringListDecoder: Decoder<List<String>> {
    override fun decode(value: Any?): List<String>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class BigDecimalListDecoder: Decoder<List<BigDecimal>> {
    override fun decode(value: Any?): List<BigDecimal>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class DateListDecoder: Decoder<List<Date>> {
    override fun decode(value: Any?): List<Date>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class TimestampListDecoder: Decoder<List<Timestamp>> {
    override fun decode(value: Any?): List<Timestamp>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class TimeListDecoder: Decoder<List<Time>> {
    override fun decode(value: Any?): List<Time>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class PgObjectListDecoder: Decoder<List<PGobject>> {
    override fun decode(value: Any?): List<PGobject>? {
        if (value is java.sql.Array) {
            return value.toList()
        }
        decodeError<java.sql.Array>(value)
    }
}

class BooleanArrayDecoder: Decoder<BooleanArray> {
    override fun decode(value: Any?): BooleanArray? {
        if (value is java.sql.Array) {
            if (value.array == null) {
                return null
            }
            val array = value.array as Array<*>
            return BooleanArray(array.size) { i ->
                val primitiveValue = array[i] ?: decodeError<Boolean>(array[i])
                primitiveValue as? Boolean ?: decodeError<Boolean>(primitiveValue)
            }
        }
        decodeError<java.sql.Array>(value)
    }
}

class ByteArrayDecoder: Decoder<ByteArray> {
    override fun decode(value: Any?): ByteArray? {
        if (value is java.sql.Array) {
            if (value.array == null) {
                return null
            }
            val array = value.array as Array<*>
            return ByteArray(array.size) { i ->
                val primitiveValue = array[i] ?: decodeError<Byte>(array[i])
                primitiveValue as? Byte ?: decodeError<Byte>(primitiveValue)
            }
        }
        decodeError<java.sql.Array>(value)
    }
}

class ShortArrayDecoder: Decoder<ShortArray> {
    override fun decode(value: Any?): ShortArray? {
        if (value is java.sql.Array) {
            if (value.array == null) {
                return null
            }
            val array = value.array as Array<*>
            return ShortArray(array.size) { i ->
                val primitiveValue = array[i] ?: decodeError<Short>(array[i])
                primitiveValue as? Short ?: decodeError<Short>(primitiveValue)
            }
        }
        decodeError<java.sql.Array>(value)
    }
}

class IntArrayDecoder: Decoder<IntArray> {
    override fun decode(value: Any?): IntArray? {
        if (value is java.sql.Array) {
            if (value.array == null) {
                return null
            }
            val array = value.array as Array<*>
            return IntArray(array.size) { i ->
                val primitiveValue = array[i] ?: decodeError<Int>(array[i])
                primitiveValue as? Int ?: decodeError<Int>(primitiveValue)
            }
        }
        decodeError<java.sql.Array>(value)
    }
}

class LongArrayDecoder: Decoder<LongArray> {
    override fun decode(value: Any?): LongArray? {
        if (value is java.sql.Array) {
            if (value.array == null) {
                return null
            }
            val array = value.array as Array<*>
            return LongArray(array.size) { i ->
                val primitiveValue = array[i] ?: decodeError<Long>(array[i])
                primitiveValue as? Long ?: decodeError<Long>(primitiveValue)
            }
        }
        decodeError<java.sql.Array>(value)
    }
}

class FloatArrayDecoder: Decoder<FloatArray> {
    override fun decode(value: Any?): FloatArray? {
        if (value is java.sql.Array) {
            if (value.array == null) {
                return null
            }
            val array = value.array as Array<*>
            return FloatArray(array.size) { i ->
                val primitiveValue = array[i] ?: decodeError<Float>(array[i])
                primitiveValue as? Float ?: decodeError<Float>(primitiveValue)
            }
        }
        decodeError<java.sql.Array>(value)
    }
}

class DoubleArrayDecoder: Decoder<DoubleArray> {
    override fun decode(value: Any?): DoubleArray? {
        if (value is java.sql.Array) {
            if (value.array == null) {
                return null
            }
            val array = value.array as Array<*>
            return DoubleArray(array.size) { i ->
                val primitiveValue = array[i] ?: decodeError<Double>(array[i])
                primitiveValue as? Double ?: decodeError<Double>(primitiveValue)
            }
        }
        decodeError<java.sql.Array>(value)
    }
}

class InstantArrayDecoder: Decoder<Array<Instant>> {
    override fun decode(value: Any?): Array<Instant>? {
        if (value is java.sql.Array) {
            return value.toArray()
        }
        decodeError<Array<Instant>>(value)
    }
}

class StringArrayDecoder: Decoder<Array<String>> {
    override fun decode(value: Any?): Array<String>? {
        if (value is java.sql.Array) {
            return value.toArray()
        }
        decodeError<Array<String>>(value)
    }
}

class BigDecimalArrayDecoder: Decoder<Array<BigDecimal>> {
    override fun decode(value: Any?): Array<BigDecimal>? {
        if (value is java.sql.Array) {
            return value.toArray()
        }
        decodeError<Array<BigDecimal>>(value)
    }
}

class DateArrayDecoder: Decoder<Array<Date>> {
    override fun decode(value: Any?): Array<Date>? {
        if (value is java.sql.Array) {
            return value.toArray()
        }
        decodeError<Array<Date>>(value)
    }
}

class TimestampArrayDecoder: Decoder<Array<Timestamp>> {
    override fun decode(value: Any?): Array<Timestamp>? {
        if (value is java.sql.Array) {
            return value.toArray()
        }
        decodeError<Array<Timestamp>>(value)
    }
}

class TimeArrayDecoder: Decoder<Array<Time>> {
    override fun decode(value: Any?): Array<Time>? {
        if (value is java.sql.Array) {
            return value.toArray()
        }
        decodeError<Array<Time>>(value)
    }
}

class NullableBooleanArrayDecoder: Decoder<Array<Boolean?>> {
    override fun decode(value: Any?): Array<Boolean?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<java.sql.Array>(value)
    }
}

class NullableByteArrayDecoder: Decoder<Array<Byte?>> {
    override fun decode(value: Any?): Array<Byte?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<java.sql.Array>(value)
    }
}

class NullableShortArrayDecoder: Decoder<Array<Short?>> {
    override fun decode(value: Any?): Array<Short?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<java.sql.Array>(value)
    }
}

class NullableIntArrayDecoder: Decoder<Array<Int?>> {
    override fun decode(value: Any?): Array<Int?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<java.sql.Array>(value)
    }
}

class NullableLongArrayDecoder: Decoder<Array<Long?>> {
    override fun decode(value: Any?): Array<Long?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<java.sql.Array>(value)
    }
}

class NullableFloatArrayDecoder: Decoder<Array<Float?>> {
    override fun decode(value: Any?): Array<Float?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<java.sql.Array>(value)
    }
}

class NullableDoubleArrayDecoder: Decoder<Array<Double?>> {
    override fun decode(value: Any?): Array<Double?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<java.sql.Array>(value)
    }
}

class NullableInstantArrayDecoder: Decoder<Array<Instant?>> {
    override fun decode(value: Any?): Array<Instant?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<Array<Instant>>(value)
    }
}

class NullableStringArrayDecoder: Decoder<Array<String?>> {
    override fun decode(value: Any?): Array<String?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<Array<String>>(value)
    }
}

class NullableBigDecimalArrayDecoder: Decoder<Array<BigDecimal?>> {
    override fun decode(value: Any?): Array<BigDecimal?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<Array<BigDecimal>>(value)
    }
}

class NullableDateArrayDecoder: Decoder<Array<Date?>> {
    override fun decode(value: Any?): Array<Date?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<Array<Date>>(value)
    }
}

class NullableTimestampArrayDecoder: Decoder<Array<Timestamp?>> {
    override fun decode(value: Any?): Array<Timestamp?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<Array<Timestamp>>(value)
    }
}

class NullableTimeArrayDecoder: Decoder<Array<Time?>> {
    override fun decode(value: Any?): Array<Time?>? {
        if (value is java.sql.Array) {
            return value.toArrayWithNulls()
        }
        decodeError<Array<Time>>(value)
    }
}

@Suppress("UNCHECKED_CAST")
inline fun <reified T : Any> java.sql.Array.toArray(): Array<T>? {
    val result = toArrayWithNulls<T>() ?: return null
    if (result.any { it == null }) {
        throw IllegalStateException("SQL array must not contain nulls")
    }
    return result as Array<T>
}

inline fun <reified T : Any> java.sql.Array.toArrayWithNulls(): Array<T?>? {
    if (this.array == null) {
        return null
    }
    val array = this.array as Array<*>
    val decoder = SnappyMapper.decoderCache.getOrDefault<T>()
    return Array(array.size) { i ->
        val value = array[i] ?: return@Array null
        decoder.decode(value) as? T ?: decodeError<T>(value)
    }
}

@Suppress("UNCHECKED_CAST")
inline fun <reified T : Any> java.sql.Array.toList(): List<T>? {
    val result = toListWithNulls<T>() ?: return null
    if (result.any { it == null }) {
        throw IllegalStateException("SQL array must not contain nulls")
    }
    return result as List<T>
}

inline fun <reified T : Any> java.sql.Array.toListWithNulls(): List<T?>? {
    if (this.array == null) {
        return null
    }
    val array = this.array as Array<*>
    val decoder = SnappyMapper.decoderCache.getOrDefault<T>()
    return array.map {
        if (it == null) return@map null
        decoder.decode(it) as? T ?: decodeError<T>(it)
    }
}
