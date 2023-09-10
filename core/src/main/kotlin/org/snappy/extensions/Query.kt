package org.snappy.extensions

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.RowParser
import org.snappy.RowParserCache
import org.snappy.StatementType
import java.sql.Connection

/**
 * Implementation of querying a connection for a [Sequence] of [T] rows. Prepares a statement using
 * the provided parameters, yielding [java.sql.ResultSet] rows, parsed using [rowParser] into the
 * required type [T]. Note, this [Sequence] wraps the statement and result parsing to both resources
 * are closed when an error occurs.
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 */
@PublishedApi
internal fun <T> queryImpl(
    connection: Connection,
    rowParser: RowParser<T>,
    sql: String,
    parameters: List<Any>,
    statementType: StatementType,
    timeout: UInt?,
): Sequence<T> = sequence {
    connection.getStatement(sql, parameters, statementType, timeout).use { preparedStatement ->
        preparedStatement.executeQuery().use { rs ->
            while (rs.next()) {
                val row = rs.toSnappyRow(rs.columNames)
                yield(rowParser.parseRow(row))
            }
        }
    }
}.constrainOnce()

/**
 * Execute a query against this [Connection], yielding rows from the result as a [Sequence] of [T].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 */
inline fun <reified T : Any> Connection.query(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): Sequence<T> {
    val rowParser = RowParserCache.getOrDefault<T>()
    return queryImpl(this, rowParser, sql, parameters, statementType, timeout)
}

/**
 * Execute a query against this [Connection], yielding rows from the result as a [Sequence] of [T].
 * Suspends a call to [query] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 */
suspend inline fun <reified T : Any> Connection.querySuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): Sequence<T> = withContext(Dispatchers.IO) {
    query(sql, parameters, statementType, timeout)
}
