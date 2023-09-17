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

inline fun <reified T> List<T>.toPgArray(connection: PGConnection): Encode {
    val typeName = when (val listClass = T::class) {
        Boolean::class -> return BooleanArray(this.size) { this[it] as Boolean }.toPgArray()
        Short::class -> return ShortArray(this.size) { this[it] as Short }.toPgArray()
        Int::class -> return IntArray(this.size) { this[it] as Int }.toPgArray()
        Long::class -> return LongArray(this.size) { this[it] as Long }.toPgArray()
        Float::class -> return FloatArray(this.size) { this[it] as Float }.toPgArray()
        Double::class -> return DoubleArray(this.size) { this[it] as Double }.toPgArray()
        Byte::class -> return ByteArray(this.size) { this[it] as Byte }.toPgArray()
        Instant::class -> {
            val array = connection.createArrayOf(
                "timestamp",
                this.map { Timestamp((it as Instant).toEpochMilli()) },
            )
            return Encode { preparedStatement, parameterIndex ->
                preparedStatement.setArray(parameterIndex, array)
            }
        }
        String::class -> "text"
        BigDecimal::class -> "numeric"
        Date::class -> "date"
        Timestamp::class -> "timestamp"
        Time::class -> "time"
        else -> {
            if (!listClass.isSubclassOf(ToPgObject::class)) {
                throw CannotEncodeArray(listClass)
            }
            val typeName = listClass.findAnnotation<PgType>()?.name
                ?: listClass.simpleName
                ?: listClass.jvmName
            val objectCollection = this.asSequence()
                .map { (it as ToPgObject).toPgObject() }
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

inline fun <reified T> Array<T>.toPgArray(connection: PGConnection): Encode {
    val typeName =  when (val arrayClass = T::class) {
        Boolean::class -> return BooleanArray(this.size) { this[it] as Boolean }.toPgArray()
        Short::class -> return ShortArray(this.size) { this[it] as Short }.toPgArray()
        Int::class -> return IntArray(this.size) { this[it] as Int }.toPgArray()
        Long::class -> return LongArray(this.size) { this[it] as Long }.toPgArray()
        Float::class -> return FloatArray(this.size) { this[it] as Float }.toPgArray()
        Double::class -> return DoubleArray(this.size) { this[it] as Double }.toPgArray()
        Byte::class -> return ByteArray(this.size) { this[it] as Byte }.toPgArray()
        Instant::class -> {
            val array = connection.createArrayOf(
                "timestamp",
                this.map { Timestamp((it as Instant).toEpochMilli()) },
            )
            return Encode { preparedStatement, parameterIndex ->
                preparedStatement.setArray(parameterIndex, array)
            }
        }
        String::class -> "text"
        BigDecimal::class -> "numeric"
        Date::class -> "date"
        Timestamp::class -> "timestamp"
        Time::class -> "time"
        else -> {
            if (!arrayClass.isSubclassOf(ToPgObject::class)) {
                throw CannotEncodeArray(arrayClass)
            }
            val typeName = arrayClass.findAnnotation<PgType>()?.name
                ?: arrayClass.simpleName
                ?: arrayClass.toString()
            val objectCollection = this.asSequence()
                .map { it as ToPgObject }
                .map { it.toPgObject() }
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
