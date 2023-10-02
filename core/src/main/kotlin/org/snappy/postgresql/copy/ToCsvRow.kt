package org.snappy.postgresql.copy

/**
 * Interface for converting an object to a [List] of [String] values for usage in a COPY Of CSV data
 * to a postgres server.
 */
fun interface ToCsvRow {
    /** Returns the properties of an object as a [List] of [String] values */
    fun toCsvRow(): Array<String>
}
