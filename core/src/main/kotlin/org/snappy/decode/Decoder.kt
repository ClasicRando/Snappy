package org.snappy.decode

import org.snappy.NullSet
import org.snappy.rowparse.SnappyRow
import kotlin.reflect.KType

/**
 * Interface to decode a single value returned when parsing a [SnappyRow] column value into the
 * desired type [T]. This can be helpful when working with Composite types in Postgresql, objects in
 * Oracle, table valued parameters in SQL Server, etc.
 */
fun interface Decoder<T : Any> {
    /** Convert a field to the required type [T], given the [fieldName] and [row] */
    fun decode(row: SnappyRow, fieldName: String): T?
}

/**
 * Decode a field given the type information of target. This allows for a nullable field to be
 * properly assigned
 */
internal fun <T : Any> Decoder<T>.decodeWithType(
    valueType: KType,
    propName: String,
    row: SnappyRow,
    fieldName: String
): T? {
    return decode(row, fieldName)
        ?: if (valueType.isMarkedNullable) {
            null
        } else {
            throw NullSet(propName)
        }
}
