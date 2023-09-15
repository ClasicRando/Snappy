package org.snappy.batch

import org.snappy.statement.SqlParameter
import org.snappy.extensions.toSqlParameterList

/**
 * Functional interface to allow parsing an object into a batch of parameters passed as part of call
 * to [executeBatch] or [executeLargeBatch] (or their suspend variations).
 */
fun interface ParameterBatch {
    /**
     * Convert the current instance to a [List] of objects (later parsed into a [List] of
     * [SqlParameter] if the object instances are not of that type). Note the order within the
     * [List] is the order the parameters are bound to a [java.sql.PreparedStatement].
     */
    fun toParameterBatch(): List<Any?>
}

/**
 * Convert a [ParameterBatch] into a [List] of [SqlParameter]. Calls
 * [ParameterBatch.toParameterBatch] then [List.toSqlParameterList] to get the result.
 */
internal fun ParameterBatch.toSqlParameterBatch(): List<SqlParameter> {
    return toParameterBatch().toSqlParameterList()
}
