package org.snappy.postgresql.array

import org.postgresql.PGConnection
import org.snappy.encode.Encode
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
import kotlin.reflect.full.isSubclassOf
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

inline fun <reified T> Collection<T>.toPgArray(connection: PGConnection): Encode {
    return this.toTypedArray().toPgArray(connection)
}

inline fun <reified T> Array<T>.toPgArray(connection: PGConnection): Encode {
    val typeName = when (val listClass = T::class) {
        Boolean::class -> "bool"
        Short::class -> "smallint"
        Int::class -> "int"
        Long::class -> "bigint"
        Float::class -> "float4"
        Double::class -> "float8"
        Byte::class -> "bytea"
        String::class -> "text"
        BigDecimal::class -> "numeric"
        Date::class -> "date"
        Timestamp::class -> "timestamp"
        Time::class -> "time"
        LocalTime::class -> "time"
        OffsetTime::class -> "timetz"
        LocalDate::class -> "date"
        LocalDateTime::class -> "timestamp"
        OffsetDateTime::class -> "timestamptz"
        Instant::class -> "timestamptz"
        else -> {
            if (!listClass.isSubclassOf(ToPgObject::class)) {
                throw CannotEncodeArray(listClass)
            }
            val typeName = listClass.findAnnotation<PgType>()?.name
                ?: listClass.simpleName
                ?: listClass.jvmName
            val objectCollection = Array(this.size) {
                (this[it] as ToPgObject).toPgObject()
            }
            val array = connection.createArrayOf(typeName, objectCollection)
            return Encode { preparedStatement, parameterIndex ->
                preparedStatement.setArray(parameterIndex, array)
            }
        }
    }
    val array = connection.createArrayOf(typeName, this)
    return Encode { preparedStatement, parameterIndex ->
        preparedStatement.setArray(parameterIndex, array)
    }
}
