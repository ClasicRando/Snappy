package org.snappy.rowparse

/**
 * Main interface allowing [java.sql.ResultSet] objects to be parsed into a more meaningful type
 * [T]. Declaring a [RowParser] type signifies that the type [T] can be extracted from a
 * [java.sql.ResultSet]. For convince, [java.sql.ResultSet] objects are pre-parsed into a
 * generalized row type, [SnappyRow], for easier extraction of results without interfacing with the
 * underlining [java.sql.ResultSet].
 *
 * Default parsers for simple data classes and POJOs are provided cached so implementing this
 * interface means you need custom behaviour when extracting from a SQL query result.
 */
interface RowParser<T> {
    /**
     * Parse the provided [row] into the desired type [T]. Although this function can fail for any
     * reason since the implementor might specify their own checks within the parsing, it's failures
     * can be seen below.
     *
     * @exception IllegalStateException expected state of the data within the [row], this generally
     * happens when an exception is thrown from a call to [check]
     * @exception IllegalArgumentException argument(s) to [T] constructor is the wrong type
     * @exception NullPointerException value used to create [T] was null but non-null type required
     */
    fun parseRow(row: SnappyRow): T
}
