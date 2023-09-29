package org.snappy.postgresql.array

import org.postgresql.util.PGobject
import org.snappy.encode.Encode
import org.snappy.postgresql.literal.toPgArrayLiteral
import org.snappy.postgresql.type.PgType
import org.snappy.postgresql.type.ToPgObject
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.jvm.jvmName

fun BooleanArray.toPgArray(): Encode {
    return Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, this)
    }
}

fun ShortArray.toPgArray(): Encode {
    return Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, this)
    }
}

fun IntArray.toPgArray(): Encode {
    return Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, this)
    }
}

fun LongArray.toPgArray(): Encode {
    return Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, this)
    }
}

fun FloatArray.toPgArray(): Encode {
    return Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, this)
    }
}

fun DoubleArray.toPgArray(): Encode {
    return Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, this)
    }
}

fun ByteArray.toPgArray(): Encode {
    return Encode { preparedStatement, parameterIndex ->
        preparedStatement.setObject(parameterIndex, this)
    }
}

inline fun <reified T> Collection<T>.toPgArray(): Encode {
    return this.toTypedArray().toPgArray()
}

inline fun <reified T> Array<T>.toPgArray(): Encode {
    val typeName = when (val listClass = T::class) {
        Boolean::class -> "_bool"
        Short::class -> "_int2"
        Int::class -> "_int4"
        Long::class -> "_int8"
        Float::class -> "_float4"
        Double::class -> "_float8"
        Byte::class -> "_bytea"
        String::class -> "_text"
        BigDecimal::class -> "_numeric"
        Date::class -> "_date"
        Timestamp::class -> "_timestamp"
        Time::class -> "_time"
        LocalTime::class -> "_time"
        OffsetTime::class -> "_timetz"
        LocalDate::class -> "_date"
        LocalDateTime::class -> "_timestamp"
        OffsetDateTime::class -> "_timestamptz"
        Instant::class -> "_timestamptz"
        else -> {
            val typeName = listClass.findAnnotation<PgType>()?.name
                ?: listClass.simpleName
                ?: listClass.jvmName
            "_$typeName"
        }
    }
    val pgObject = PGobject().apply {
        value = this@toPgArray.toPgArrayLiteral()
        type = typeName
    }
    return ToPgObject { pgObject }
}
