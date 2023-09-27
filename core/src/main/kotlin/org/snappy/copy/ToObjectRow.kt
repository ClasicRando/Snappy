package org.snappy.copy

/**
 * Interface for converting an object to a [List] of object values for usage in a copy of data to a
 * database server.
 */
fun interface ToObjectRow {
    /** Returns the properties of an object as a [List] of object values */
    fun toObjectRow(): List<Any?>
}
