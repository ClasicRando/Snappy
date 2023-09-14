package org.snappy.encode

import java.math.BigDecimal
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.Time
import java.sql.Timestamp

@JvmInline
private value class SqlByte(private val byte: Byte) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setByte(parameterIndex, byte)
    }
}

@JvmInline
private value class SqlShort(private val short: Short) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setShort(parameterIndex, short)
    }
}

@JvmInline
private value class SqlInt(private val int: Int) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setInt(parameterIndex, int)
    }
}

@JvmInline
private value class SqlLong(private val long: Long) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setLong(parameterIndex, long)
    }
}

@JvmInline
private value class SqlFloat(private val float: Float) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setFloat(parameterIndex, float)
    }
}

@JvmInline
private value class SqlDouble(private val double: Double) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setDouble(parameterIndex, double)
    }
}

@JvmInline
private value class SqlBigDecimal(private val bigDecimal: BigDecimal) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setBigDecimal(parameterIndex, bigDecimal)
    }
}

@JvmInline
private value class SqlDate(private val date: Date) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setDate(parameterIndex, date)
    }
}

@JvmInline
private value class SqlTimestamp(private val timestamp: Timestamp) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setTimestamp(parameterIndex, timestamp)
    }
}

@JvmInline
private value class SqlTime(private val time: Time) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setTime(parameterIndex, time)
    }
}

@JvmInline
private value class SqlString(private val string: String) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setString(parameterIndex, string)
    }
}

@JvmInline
private value class SqlByteArray(private val byteArray: ByteArray) : Encode {
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setBytes(parameterIndex, byteArray)
    }
}

fun toEncodable(input: Any?): Encode = when (input) {
    is Encode -> input
    is Byte -> SqlByte(input)
    is Short -> SqlShort(input)
    is Int -> SqlInt(input)
    is Long -> SqlLong(input)
    is Float -> SqlFloat(input)
    is Double -> SqlDouble(input)
    is BigDecimal -> SqlBigDecimal(input)
    is Date -> SqlDate(input)
    is Timestamp -> SqlTimestamp(input)
    is Time -> SqlTime(input)
    is String -> SqlString(input)
    is ByteArray -> SqlByteArray(input)
    else -> Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, input)
    }
}
