package org.snappy

/**
 * Main interface allowing [java.sql.ResultSet] objects to be parsed into a more meaningful type
 * [T]. Declaring a [RowParser] type signifies that the type [T] can be extracted from a
 * [java.sql.ResultSet]. For convince, [java.sql.ResultSet] objects are pre-parsed into a
 * generalized row type, [SnappyRow], for easier extraction of results without interfacing with the
 * underlining [java.sql.ResultSet].
 *
 * Default parsers for simple data classes and POJOs are provided cached so implementing this
 * interface means you need custom behaviour when extracting from a SQL query result.
 *
 * When implementing the interface, keep in mind that the [SnappyRow] has convenience methods for
 * extracting row values so accessing the underlining [SnappyRow.data] map should never be done.
 * In most cases, you will only need to perform a simple checked cast against the map's key, which
 * can be done using the [SnappyRow.getAs] function. If you need to extract a boxed object, use the
 * [SnappyRow.get] function to allow for more complex parsing of data into the desired type.
 */
interface RowParser<T> {
    /**
     * Parse the provided [row] into the desired type [T]. Although this function can fail for any
     * reason since the implementor might specify their own checks within the parsing, it's failures
     * can be seen below.
     *
     * @exception NullRowValue not-null assertion on field parsing failed
     * @exception IllegalStateException expected state of the data within the [row], this generally
     * happens when an exception is thrown from a call to [check]
     * @exception IllegalArgumentException argument(s) to [T] constructor is the wrong type
     * @exception NullPointerException value used to create [T] was null but non-null type required
     */
    fun parseRow(row: SnappyRow): T
}
