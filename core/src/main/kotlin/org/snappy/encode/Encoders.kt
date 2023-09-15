package org.snappy.encode

import java.math.BigDecimal
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant

/** Wrapper for [Boolean] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlBoolean(private val boolean: Boolean) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setBoolean(parameterIndex, boolean)
    }
}

/** Wrapper for [Byte] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlByte(private val byte: Byte) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setByte(parameterIndex, byte)
    }
}

/** Wrapper for [Short] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlShort(private val short: Short) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setShort(parameterIndex, short)
    }
}

/** Wrapper for [Int] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlInt(private val int: Int) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setInt(parameterIndex, int)
    }
}

/** Wrapper for [Long] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlLong(private val long: Long) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setLong(parameterIndex, long)
    }
}

/** Wrapper for [Float] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlFloat(private val float: Float) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setFloat(parameterIndex, float)
    }
}

/** Wrapper for [Double] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlDouble(private val double: Double) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setDouble(parameterIndex, double)
    }
}

/** Wrapper for [BigDecimal] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlBigDecimal(private val bigDecimal: BigDecimal) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setBigDecimal(parameterIndex, bigDecimal)
    }
}

/** Wrapper for [Date] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlDate(private val date: Date) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setDate(parameterIndex, date)
    }
}

/** Wrapper for [Timestamp] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlTimestamp(private val timestamp: Timestamp) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setTimestamp(parameterIndex, timestamp)
    }
}

/** Wrapper for [Time] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlTime(private val time: Time) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setTime(parameterIndex, time)
    }
}

/** Wrapper for [String] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlString(private val string: String) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setString(parameterIndex, string)
    }
}

/** Wrapper for [ByteArray] to allow for encoding to a [PreparedStatement] */
@JvmInline
private value class SqlByteArray(private val byteArray: ByteArray) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setBytes(parameterIndex, byteArray)
    }
}

/**
 * Convert an [input] object to a wrapper value that implements [Encode] for
 * [java.sql.PreparedStatement] parameter binding. If [input] is already a value that implements
 * [Encode] the value is returned without wrapping.
 */
fun toEncodable(input: Any?): Encode = when (input) {
    is Encode -> input
    is Boolean -> SqlBoolean(input)
    is Byte -> SqlByte(input)
    is Short -> SqlShort(input)
    is Int -> SqlInt(input)
    is Long -> SqlLong(input)
    is Float -> SqlFloat(input)
    is Double -> SqlDouble(input)
    is BigDecimal -> SqlBigDecimal(input)
    is Date -> SqlDate(input)
    is Timestamp -> SqlTimestamp(input)
    is Instant -> SqlTimestamp(Timestamp(input.epochSecond))
    is Time -> SqlTime(input)
    is String -> SqlString(input)
    is ByteArray -> SqlByteArray(input)
    else -> Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, input)
    }
}
