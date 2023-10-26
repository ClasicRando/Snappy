package org.snappy.rowparse

import java.math.BigDecimal
import java.sql.Array
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime

/**
 * Internal representation of a row from a [java.sql.ResultSet] to not expose the result to a user.
 * Access of data is through the field name.
 */
interface SnappyRow {
    /** Number of columns in the row */
    val size: Int
    /** Read-only view of the row entries */
    val entries: Sequence<Pair<String, Any?>>
    /** Check if a row contains the specified [key] */
    fun containsKey(key: String): Boolean
    /** */
    fun getStringNullable(key: String): String?
    /** */
    fun getString(key: String): String
    /** */
    fun getBooleanNullable(key: String): Boolean?
    /** */
    fun getBoolean(key: String): Boolean
    /** */
    fun getByteNullable(key: String): Byte?
    /** */
    fun getByte(key: String): Byte
    /** */
    fun getShortNullable(key: String): Short?
    /** */
    fun getShort(key: String): Short
    /** */
    fun getIntNullable(key: String): Int?
    /** */
    fun getInt(key: String): Int
    /** */
    fun getLongNullable(key: String): Long?
    /** */
    fun getLong(key: String): Long
    /** */
    fun getFloatNullable(key: String): Float?
    /** */
    fun getFloat(key: String): Float
    /** */
    fun getDoubleNullable(key: String): Double?
    /** */
    fun getDouble(key: String): Double
    /** */
    fun getBigDecimalNullable(key: String): BigDecimal?
    /** */
    fun getBigDecimal(key: String): BigDecimal
    /** */
    fun getBytesNullable(key: String): ByteArray?
    /** */
    fun getBytes(key: String): ByteArray
    /** */
    fun getDateNullable(key: String): Date?
    /** */
    fun getDate(key: String): Date
    /** */
    fun getTimeNullable(key: String): Time?
    /** */
    fun getTime(key: String): Time
    /** */
    fun getTimestampNullable(key: String): Timestamp?
    /** */
    fun getTimestamp(key: String): Timestamp
    /** */
    fun getLocalDateNullable(key: String): LocalDate?
    /** */
    fun getLocalDate(key: String): LocalDate
    /** */
    fun getLocalTimeNullable(key: String): LocalTime?
    /** */
    fun getLocalTime(key: String): LocalTime
    /** */
    fun getLocalDateTimeNullable(key: String): LocalDateTime?
    /** */
    fun getLocalDateTime(key: String): LocalDateTime
    /** */
    fun getOffsetDateTimeNullable(key: String): OffsetDateTime?
    /** */
    fun getOffsetDateTime(key: String): OffsetDateTime
    /** */
    fun getOffsetTimeNullable(key: String): OffsetTime?
    /** */
    fun getOffsetTime(key: String): OffsetTime
    /** */
    fun getInstantNullable(key: String): Instant?
    /** */
    fun getInstant(key: String): Instant
    /** */
    fun getAnyNullable(key: String): Any?
    /** */
    fun getAny(key: String): Any
    /** */
    fun <T : Any> getObjectNullable(key: String, type: Class<T>): T?
    /** */
    fun <T : Any> getObject(key: String, type: Class<T>): T
    /** */
    fun getArrayNullable(key: String): Array?
    /** */
    fun getArray(key: String): Array
}

/** */
inline fun <reified T : Any> SnappyRow.getObjectNullable(key: String): T? {
    return getObjectNullable(key, T::class.java)
}

/** */
inline fun <reified T : Any> SnappyRow.getObject(key: String): T {
    return getObject(key, T::class.java)
}
