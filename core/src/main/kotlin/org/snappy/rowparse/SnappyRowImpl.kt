package org.snappy.rowparse

import java.math.BigDecimal
import java.sql.Array
import java.sql.Date
import java.sql.ResultSet
import java.sql.Time
import java.sql.Timestamp

/**
 * Internal representation of a row from a [java.sql.ResultSet] to not expose the result to a user.
 * Access of data is through the field name.
 */
class SnappyRowImpl(
    private val resultSet: ResultSet,
    private val columnNames: List<String>,
) : SnappyRow {

    /** Number of columns in the row */
    override val size: Int = resultSet.metaData.columnCount
    /** Read-only view of the row entries */
    override val entries: Sequence<Pair<String, Any?>>
        get() = columnNames.asSequence().map { name -> name to resultSet.getObject(name) }

    /** Check if a row contains the specified [key] */
    override fun containsKey(key: String) = columnNames.contains(key)

    override fun getStringNullable(key: String): String? = resultSet.getString(key)

    override fun getString(key: String): String = resultSet.getString(key)
        ?: error("Expected not null string value but found null")

    override fun getBooleanNullable(key: String): Boolean? {
        val result = resultSet.getBoolean(key)
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getBoolean(key: String): Boolean = getBooleanNullable(key)
        ?: error("Expected not null boolean value but found null")

    override fun getByteNullable(key: String): Byte? {
        val result = resultSet.getByte(key)
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getByte(key: String): Byte = getByteNullable(key)
        ?: error("Expected not null byte value but found null")

    override fun getShortNullable(key: String): Short? {
        val result = resultSet.getShort(key)
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getShort(key: String): Short = getShortNullable(key)
        ?: error("Expected not null short value but found null")

    override fun getIntNullable(key: String): Int? {
        val result = resultSet.getInt(key)
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getInt(key: String): Int = getIntNullable(key)
        ?: error("Expected not null int value but found null")

    override fun getLongNullable(key: String): Long? {
        val result = resultSet.getLong(key)
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getLong(key: String): Long = getLongNullable(key)
        ?: error("Expected not null long value but found null")

    override fun getFloatNullable(key: String): Float? {
        val result = resultSet.getFloat(key)
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getFloat(key: String): Float = getFloatNullable(key)
        ?: error("Expected not null float value but found null")

    override fun getDoubleNullable(key: String): Double? {
        val result = resultSet.getDouble(key)
        if (resultSet.wasNull()) {
            return null
        }
        return result
    }

    override fun getDouble(key: String): Double = getDoubleNullable(key)
        ?: error("Expected not null double value but found null")

    override fun getBigDecimalNullable(key: String): BigDecimal? = resultSet.getBigDecimal(key)

    override fun getBigDecimal(key: String): BigDecimal = resultSet.getBigDecimal(key)
        ?: error("Expected not null big decimal value but found null")

    override fun getBytesNullable(key: String): ByteArray? = resultSet.getBytes(key)

    override fun getBytes(key: String): ByteArray = resultSet.getBytes(key)
        ?: error("Expected not null byte array value but found null")

    override fun getDateNullable(key: String): Date? = resultSet.getDate(key)

    override fun getDate(key: String): Date = resultSet.getDate(key)
        ?: error("Expected not null date value but found null")

    override fun getTimeNullable(key: String): Time? = resultSet.getTime(key)

    override fun getTime(key: String): Time = resultSet.getTime(key)
        ?: error("Expected not null time value but found null")

    override fun getTimestampNullable(key: String): Timestamp? = resultSet.getTimestamp(key)

    override fun getTimestamp(key: String): Timestamp = resultSet.getTimestamp(key)
        ?: error("Expected not null timestamp value but found null")

    override fun getAnyNullable(key: String): Any? = resultSet.getObject(key)

    override fun getAny(key: String): Any = resultSet.getObject(key)
        ?: error("Expected not null object value but found null")

    override fun <T : Any> getObjectNullable(key: String, type: Class<T>): T? {
        return resultSet.getObject(key, type)
    }

    override fun <T : Any> getObject(key: String, type: Class<T>): T {
        return resultSet.getObject(key, type)
            ?: error("Expected not null object value but found null")
    }

    override fun getArray(key: String): Array = resultSet.getArray(key)
}
