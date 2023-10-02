package org.snappy.decode

import org.snappy.NullSet
import org.snappy.rowparse.SnappyRow

/**
 * Interface to decode a single value returned when parsing a [SnappyRow] column value into the
 * desired type [T]. This can be helpful when working with Composite types in Postgresql, objects in
 * Oracle, table valued parameters in SQL Server, etc.
 */
fun interface Decoder<T : Any> {
    /** Convert a field to the required type [T], given the [fieldName] and [row] */
    fun decode(row: SnappyRow, fieldName: String): T {
        return decodeNullable(row, fieldName) ?: throw NullSet(fieldName)
    }

    fun decodeNullable(row: SnappyRow, fieldName: String): T?
}
