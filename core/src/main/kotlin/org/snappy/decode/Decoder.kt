package org.snappy.decode

import org.snappy.NullSet
import kotlin.reflect.KType

/**
 * Interface to decode a single value returned when parsing a [java.sql.ResultSet] column
 * value into the desired type [T]. This can be helpful when working with Composite types in
 * Postgresql, objects in Oracle, table valued parameters in SQL Server, etc.
 */
fun interface Decoder<T : Any> {
    /** Convert a boxed return [value] from a [java.sql.ResultSet] into the desired custom type */
    fun decode(value: Any?): T?
}

internal fun <T : Any> Decoder<T>.decodeWithType(
    valueType: KType,
    propName: String,
    value: Any?,
): T? {
    if (value == null) {
        if (valueType.isMarkedNullable) {
            return null
        } else {
            throw NullSet(propName)
        }
    }
    return decode(value)
}
