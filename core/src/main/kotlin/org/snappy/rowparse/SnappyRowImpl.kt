package org.snappy.rowparse

import org.snappy.SnappyMapper
import java.math.BigDecimal
import java.sql.Array
import java.sql.Date
import java.sql.ResultSet
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
class SnappyRowImpl(
    private val resultSet: ResultSet,
    private val columnNames: List<String>,
) : SnappyRow {
    private val columnNamesCleaned by lazy {
        columnNames.map { it.replace("_", "").lowercase() }
    }

    /** Number of columns in the row */
    override val size: Int = resultSet.metaData.columnCount
    /** Read-only view of the row entries */
    override val entries: Sequence<Pair<String, Any?>>
        get() = columnNames.asSequence().map { name -> name to resultSet.getObject(name) }

    /** Check if a row contains the specified [key] */
    override fun containsKey(key: String) = columnNames.contains(key)

    private fun getColumnIndex(key: String): Int {
        val index = columnNames.indexOf(key)
        if (index >= 0) {
            return index + 1
        }

        if (!SnappyMapper.allowUnderscoreMatch) {
            error("Could not find column for key = '$key'. Underscore matching not enabled")
        }

        return columnNamesCleaned.indexOf(key.lowercase())
            .takeIf { it >= 0 }
            ?.plus(1)
            ?: error("Could not find column for key = '$key'")
    }

    override fun getStringNullable(key: String): String? = resultSet.getString(getColumnIndex(key))

    override fun getString(key: String): String = getStringNullable(key)
        ?: error("Expected not null string value but found null")

    override fun getBooleanNullable(key: String): Boolean? {
        val result = resultSet.getBoolean(getColumnIndex(key))
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getBoolean(key: String): Boolean = getBooleanNullable(key)
        ?: error("Expected not null boolean value but found null")

    override fun getByteNullable(key: String): Byte? {
        val result = resultSet.getByte(getColumnIndex(key))
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getByte(key: String): Byte = getByteNullable(key)
        ?: error("Expected not null byte value but found null")

    override fun getShortNullable(key: String): Short? {
        val result = resultSet.getShort(getColumnIndex(key))
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getShort(key: String): Short = getShortNullable(key)
        ?: error("Expected not null short value but found null")

    override fun getIntNullable(key: String): Int? {
        val result = resultSet.getInt(getColumnIndex(key))
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getInt(key: String): Int = getIntNullable(key)
        ?: error("Expected not null int value but found null")

    override fun getLongNullable(key: String): Long? {
        val result = resultSet.getLong(getColumnIndex(key))
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getLong(key: String): Long = getLongNullable(key)
        ?: error("Expected not null long value but found null")

    override fun getFloatNullable(key: String): Float? {
        val result = resultSet.getFloat(getColumnIndex(key))
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getFloat(key: String): Float = getFloatNullable(key)
        ?: error("Expected not null float value but found null")

    override fun getDoubleNullable(key: String): Double? {
        val result = resultSet.getDouble(getColumnIndex(key))
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getDouble(key: String): Double = getDoubleNullable(key)
        ?: error("Expected not null double value but found null")

    override fun getBigDecimalNullable(key: String): BigDecimal? {
        return resultSet.getBigDecimal(getColumnIndex(key))
    }

    override fun getBigDecimal(key: String): BigDecimal {
        return resultSet.getBigDecimal(getColumnIndex(key))
            ?: error("Expected not null big decimal value but found null")
    }

    override fun getBytesNullable(key: String): ByteArray? = resultSet.getBytes(getColumnIndex(key))

    override fun getBytes(key: String): ByteArray = getBytesNullable(key)
        ?: error("Expected not null byte array value but found null")

    override fun getDateNullable(key: String): Date? = resultSet.getDate(getColumnIndex(key))

    override fun getDate(key: String): Date = getDateNullable(key)
        ?: error("Expected not null date value but found null")

    override fun getTimeNullable(key: String): Time? = resultSet.getTime(getColumnIndex(key))

    override fun getTime(key: String): Time = getTimeNullable(key)
        ?: error("Expected not null time value but found null")

    override fun getTimestampNullable(key: String): Timestamp? {
        return resultSet.getTimestamp(getColumnIndex(key))
    }

    override fun getTimestamp(key: String): Timestamp = getTimestampNullable(key)
        ?: error("Expected not null timestamp value but found null")

    override fun getLocalDateNullable(key: String): LocalDate? {
        return resultSet.getObject(getColumnIndex(key), LocalDate::class.java)
    }

    override fun getLocalDate(key: String): LocalDate {
        return getLocalDateNullable(key)
            ?: error("Expected not null local date value but found null")
    }

    override fun getLocalTimeNullable(key: String): LocalTime? {
        return resultSet.getObject(getColumnIndex(key), LocalTime::class.java)
    }

    override fun getLocalTime(key: String): LocalTime {
        return getLocalTimeNullable(key)
            ?: error("Expected not null local time value but found null")
    }

    override fun getLocalDateTimeNullable(key: String): LocalDateTime? {
        return resultSet.getObject(getColumnIndex(key), LocalDateTime::class.java)
    }

    override fun getLocalDateTime(key: String): LocalDateTime {
        return getLocalDateTimeNullable(key)
            ?: error("Expected not null local datetime value but found null")
    }

    override fun getOffsetDateTimeNullable(key: String): OffsetDateTime? {
        return resultSet.getObject(getColumnIndex(key), OffsetDateTime::class.java)
    }

    override fun getOffsetDateTime(key: String): OffsetDateTime {
        return getOffsetDateTimeNullable(key)
            ?: error("Expected not null offset datetime value but found null")
    }

    override fun getOffsetTimeNullable(key: String): OffsetTime? {
        return resultSet.getObject(getColumnIndex(key), OffsetTime::class.java)
    }

    override fun getOffsetTime(key: String): OffsetTime {
        return getOffsetTimeNullable(key)
            ?: error("Expected not null offset time value but found null")
    }

    override fun getInstantNullable(key: String): Instant? {
        return resultSet.getObject(getColumnIndex(key), Instant::class.java)
    }

    override fun getInstant(key: String): Instant {
        return getInstantNullable(key)
            ?: error("Expected not null instant value but found null")
    }

    override fun getAnyNullable(key: String): Any? = resultSet.getObject(getColumnIndex(key))

    override fun getAny(key: String): Any = getAnyNullable(key)
        ?: error("Expected not null object value but found null")

    override fun <T : Any> getObjectNullable(key: String, type: Class<T>): T? {
        return resultSet.getObject(getColumnIndex(key), type)
    }

    override fun <T : Any> getObject(key: String, type: Class<T>): T {
        return resultSet.getObject(getColumnIndex(key), type)
            ?: error("Expected not null object value but found null")
    }

    override fun getArray(key: String): Array = resultSet.getArray(getColumnIndex(key))
}
