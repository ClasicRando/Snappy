package org.snappy.decode

/**
 * Interface to decode a single value returned when parsing a [java.sql.ResultSet] column
 * value into the desired type [T]. This can be helpful when working with Composite types in
 * Postgresql, objects in Oracle, table valued parameters in SQL Server, etc.
 */
fun interface Decoder<T> {
    /** Convert a boxed return [value] from a [java.sql.ResultSet] into the desired custom type */
    fun decode(value: Any): T
}
