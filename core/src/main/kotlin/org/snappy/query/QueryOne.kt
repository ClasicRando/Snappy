package org.snappy.query

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.EmptyResult
import org.snappy.rowparse.RowParser
import org.snappy.SnappyMapper
import org.snappy.statement.StatementType
import org.snappy.TooManyRows
import org.snappy.extensions.columnNames
import org.snappy.extensions.getStatement
import org.snappy.rowparse.SnappyRowImpl
import java.sql.Connection

/**
 * Row return variants. Tells the result parser if an error should be thrown if multiple rows are
 * returned.
 */
@PublishedApi
internal enum class RowReturn {
    Single,
    First,
}

/**
 * Implementation of querying a connection for a single row. Prepares a statement using the provided
 * parameters, reading the result and parsing the row using [rowParser] into the required type [T].
 * Note, both the statement and result set are closed by default before returning from the function.
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @exception TooManyRows if [rowReturn] is [RowReturn.Single] and more than 1 row is returned
 */
@PublishedApi
internal fun <T> querySingleRowImpl(
    connection: Connection,
    rowParser: RowParser<T>,
    sql: String,
    parameters: List<Any>,
    statementType: StatementType,
    timeout: UInt?,
    rowReturn: RowReturn,
): T? {
    return connection.getStatement(sql, parameters, statementType, timeout).use { preparedStatement ->
        preparedStatement.executeQuery().use { rs ->
            if (rs.next()) {
                val row = SnappyRowImpl(rs, rs.columnNames)
                val mappedRow = rowParser.parseRow(row)
                if (rowReturn == RowReturn.Single && rs.next()) {
                    throw TooManyRows()
                }
                mappedRow
            } else {
                null
            }
        }
    }
}

/**
 * Execute a query against this [Connection], returning at most a single row of [T] (null if no rows
 * returned)
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @exception TooManyRows result contains more than 1 row
 * @see java.sql.Statement.executeQuery
 */
inline fun <reified T : Any> Connection.querySingleOrNull(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T? {
    val rowParser = SnappyMapper.rowParserCache.getOrDefault<T>()
    return querySingleRowImpl(
        this,
        rowParser,
        sql,
        parameters,
        statementType,
        timeout,
        RowReturn.Single,
    )
}

/**
 * Execute a query against this [Connection], returning a single row of [T]
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @exception TooManyRows result contains more than 1 row
 * @exception EmptyResult result contain no rows
 * @see java.sql.Statement.executeQuery
 */
inline fun <reified T : Any> Connection.querySingle(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T {
    return querySingleOrNull(sql, parameters, statementType, timeout) ?: throw EmptyResult()
}

/**
 * Execute a query against this [Connection], returning at most a single row of [T] (null if no rows
 * returned). Suspends a call to [querySingleOrNull] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @exception TooManyRows result contains more than 1 row
 * @see java.sql.Statement.executeQuery
 * @see withContext
 * @see Dispatchers.IO
 */
suspend inline fun <reified T : Any> Connection.querySingleOrNullSuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T? = withContext(Dispatchers.IO) {
    querySingleOrNull(sql, parameters, statementType, timeout)
}

/**
 * Execute a query against this [Connection], returning a single row of [T]. Suspends a call to
 * [querySingle] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @exception TooManyRows result contains more than 1 row
 * @exception EmptyResult result contain no rows
 * @see java.sql.Statement.executeQuery
 * @see withContext
 * @see Dispatchers.IO
 */
suspend inline fun <reified T : Any> Connection.querySingleSuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T = withContext(Dispatchers.IO) {
    querySingle(sql, parameters, statementType, timeout)
}

/**
 * Execute a query against this [Connection], returning the first row of [T] (null if no rows
 * returned)
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeQuery
 */
inline fun <reified T : Any> Connection.queryFirstOrNull(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T? {
    val rowParser = SnappyMapper.rowParserCache.getOrDefault<T>()
    return querySingleRowImpl(
        this,
        rowParser,
        sql,
        parameters,
        statementType,
        timeout,
        RowReturn.First,
    )
}

/**
 * Execute a query against this [Connection], returning the first row of [T].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @exception EmptyResult result contain no rows
 * @see java.sql.Statement.executeQuery
 */
inline fun <reified T : Any> Connection.queryFirst(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T {
    return queryFirstOrNull(sql, parameters, statementType, timeout) ?: throw EmptyResult()
}

/**
 * Execute a query against this [Connection], returning the first row of [T] (null if no rows
 * returned). Suspends a call to [queryFirstOrNull] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeQuery
 * @see withContext
 * @see Dispatchers.IO
 */
suspend inline fun <reified T : Any> Connection.queryFirstOrNullSuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T? = withContext(Dispatchers.IO) {
    queryFirstOrNull(sql, parameters, statementType, timeout)
}

/**
 * Execute a query against this [Connection], returning the first row of [T]. Suspends a call to
 * [queryFirst] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @exception EmptyResult result contain no rows
 * @see java.sql.Statement.executeQuery
 * @see withContext
 * @see Dispatchers.IO
 */
suspend inline fun <reified T : Any> Connection.queryFirstSuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T = withContext(Dispatchers.IO) {
    queryFirst(sql, parameters, statementType, timeout)
}
