package org.snappy.postgresql.copy

/**
 * Interface for converting an object to an [Iterable] of object values for usage in a COPY Of CSV
 * data to a postgres server. This is easier convenience if you do not need to know how the data
 * becomes serialized to a [String] value before passing to the server.
 *
 * @see formatObject
 */
fun interface ToObjectRow {
    /** Returns the properties of an object as an [Iterable] of object values */
    fun toObjectRow(): Iterable<Any?>
}
