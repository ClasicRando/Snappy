package org.snappy.postgresql.array

import org.snappy.SnappyMapper
import org.snappy.decodeError

@Suppress("UNCHECKED_CAST")
inline fun <reified T : Any> java.sql.Array.toArray(): Array<T> {
    val result = toListWithNulls<T>()
    if (result.any { it == null }) {
        throw IllegalStateException("SQL array must not contain nulls")
    }
    return result as Array<T>
}

inline fun <reified T : Any> java.sql.Array.toArrayWithNulls(): Array<T?> {
    val array = this.array as Array<*>
    val decoder = SnappyMapper.decoderCache.getOrDefault<T>()
    return Array(array.size) { i ->
        val value = array[i] ?: return@Array null
        decoder.decode(value) as? T ?: decodeError<T>(value)
    }
}

@Suppress("UNCHECKED_CAST")
inline fun <reified T : Any> java.sql.Array.toList(): List<T> {
    val result = toListWithNulls<T>()
    if (result.any { it == null }) {
        throw IllegalStateException("SQL array must not contain nulls")
    }
    return result as List<T>
}

inline fun <reified T : Any> java.sql.Array.toListWithNulls(): List<T?> {
    val array = this.array as Array<*>
    val decoder = SnappyMapper.decoderCache.getOrDefault<T>()
    return array.map {
        if (it == null) return@map null
        decoder.decode(it) as? T ?: decodeError<T>(it)
    }
}
