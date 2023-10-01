@file:Suppress("UNUSED")
package org.snappy.postgresql.array

import org.postgresql.util.PGobject
import org.snappy.decode.Decoder
import org.snappy.decodeError
import org.snappy.rowparse.SnappyRow
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant

class BooleanListDecoder: Decoder<List<Boolean>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Boolean>? {
        return row.getArray(fieldName).toList()
    }
}

class ByteListDecoder: Decoder<List<Byte>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Byte>? {
        return row.getArray(fieldName).toList()
    }
}

class ShortListDecoder: Decoder<List<Short>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Short>? {
        return row.getArray(fieldName).toList()
    }
}

class IntListDecoder: Decoder<List<Int>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Int>? {
        return row.getArray(fieldName).toList()
    }
}

class LongListDecoder: Decoder<List<Long>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Long>? {
        return row.getArray(fieldName).toList()
    }
}

class FloatListDecoder: Decoder<List<Float>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Float>? {
        return row.getArray(fieldName).toList()
    }
}

class DoubleListDecoder: Decoder<List<Double>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Double>? {
        return row.getArray(fieldName).toList()
    }
}

class InstantListDecoder: Decoder<List<Instant>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Instant>? {
        return row.getArray(fieldName).toList()
    }
}

class StringListDecoder: Decoder<List<String>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<String>? {
        return row.getArray(fieldName).toList()
    }
}

class BigDecimalListDecoder: Decoder<List<BigDecimal>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<BigDecimal>? {
        return row.getArray(fieldName).toList()
    }
}

class DateListDecoder: Decoder<List<Date>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Date>? {
        return row.getArray(fieldName).toList()
    }
}

class TimestampListDecoder: Decoder<List<Timestamp>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Timestamp>? {
        return row.getArray(fieldName).toList()
    }
}

class TimeListDecoder: Decoder<List<Time>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<Time>? {
        return row.getArray(fieldName).toList()
    }
}

class PgObjectListDecoder: Decoder<List<PGobject>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): List<PGobject>? {
        return row.getArray(fieldName).toList()
    }
}

class BooleanArrayDecoder: Decoder<BooleanArray> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): BooleanArray? {
        val value = row.getArray(fieldName)
        if (value.array == null) {
            return null
        }
        val array = value.array as Array<*>
        return BooleanArray(array.size) { i ->
            val primitiveValue = array[i] ?: decodeError<Boolean>(array[i])
            primitiveValue as? Boolean ?: decodeError<Boolean>(primitiveValue)
        }
    }
}

class ShortArrayDecoder: Decoder<ShortArray> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): ShortArray? {
        val value = row.getArray(fieldName)
        if (value.array == null) {
            return null
        }
        val array = value.array as Array<*>
        return ShortArray(array.size) { i ->
            val primitiveValue = array[i] ?: decodeError<Short>(array[i])
            primitiveValue as? Short ?: decodeError<Short>(primitiveValue)
        }
    }
}

class IntArrayDecoder: Decoder<IntArray> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): IntArray? {
        val value = row.getArray(fieldName)
        if (value.array == null) {
            return null
        }
        val array = value.array as Array<*>
        return IntArray(array.size) { i ->
            val primitiveValue = array[i] ?: decodeError<Int>(array[i])
            primitiveValue as? Int ?: decodeError<Int>(primitiveValue)
        }
    }
}

class LongArrayDecoder: Decoder<LongArray> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): LongArray? {
        val value = row.getArray(fieldName)
        if (value.array == null) {
            return null
        }
        val array = value.array as Array<*>
        return LongArray(array.size) { i ->
            val primitiveValue = array[i] ?: decodeError<Long>(array[i])
            primitiveValue as? Long ?: decodeError<Long>(primitiveValue)
        }
    }
}

class FloatArrayDecoder: Decoder<FloatArray> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): FloatArray? {
        val value = row.getArray(fieldName)
        if (value.array == null) {
            return null
        }
        val array = value.array as Array<*>
        return FloatArray(array.size) { i ->
            val primitiveValue = array[i] ?: decodeError<Float>(array[i])
            primitiveValue as? Float ?: decodeError<Float>(primitiveValue)
        }
    }
}

class DoubleArrayDecoder: Decoder<DoubleArray> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): DoubleArray? {
        val value = row.getArray(fieldName)
        if (value.array == null) {
            return null
        }
        val array = value.array as Array<*>
        return DoubleArray(array.size) { i ->
            val primitiveValue = array[i] ?: decodeError<Double>(array[i])
            primitiveValue as? Double ?: decodeError<Double>(primitiveValue)
        }
    }
}

class InstantArrayDecoder: Decoder<Array<Instant>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Instant>? {
        return row.getArray(fieldName).toArray()
    }
}

class StringArrayDecoder: Decoder<Array<String>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<String>? {
        return row.getArray(fieldName).toArray()
    }
}

class BigDecimalArrayDecoder: Decoder<Array<BigDecimal>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<BigDecimal>? {
        return row.getArray(fieldName).toArray()
    }
}

class DateArrayDecoder: Decoder<Array<Date>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Date>? {
        return row.getArray(fieldName).toArray()
    }
}

class TimestampArrayDecoder: Decoder<Array<Timestamp>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Timestamp>? {
        return row.getArray(fieldName).toArray()
    }
}

class TimeArrayDecoder: Decoder<Array<Time>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Time>? {
        return row.getArray(fieldName).toArray()
    }
}

class NullableBooleanArrayDecoder: Decoder<Array<Boolean?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Boolean?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableByteArrayDecoder: Decoder<Array<Byte?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Byte?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableShortArrayDecoder: Decoder<Array<Short?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Short?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableIntArrayDecoder: Decoder<Array<Int?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Int?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableLongArrayDecoder: Decoder<Array<Long?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Long?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableFloatArrayDecoder: Decoder<Array<Float?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Float?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableDoubleArrayDecoder: Decoder<Array<Double?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Double?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableInstantArrayDecoder: Decoder<Array<Instant?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Instant?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableStringArrayDecoder: Decoder<Array<String?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<String?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableBigDecimalArrayDecoder: Decoder<Array<BigDecimal?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<BigDecimal?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableDateArrayDecoder: Decoder<Array<Date?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Date?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableTimestampArrayDecoder: Decoder<Array<Timestamp?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Timestamp?>? {
        return row.getArray(fieldName).toArrayWithNulls()
    }
}

class NullableTimeArrayDecoder: Decoder<Array<Time?>> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Array<Time?>? {
        return row.getArray(fieldName).toArrayWithNulls()
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
    this.array
    if (this.array == null) {
        return null
    }
    val array = this.array as Array<*>
    return Array(array.size) { i ->
        val value = array[i] ?: return@Array null
        value as? T ?: decodeError<T>(value)
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
    return array.map {
        if (it == null) return@map null
        it as? T ?: decodeError<T>(it)
    }
}
