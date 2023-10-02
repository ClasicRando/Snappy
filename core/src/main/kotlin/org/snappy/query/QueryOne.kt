package org.snappy.query

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.EmptyResult
import org.snappy.TooManyRows
import org.snappy.command.sqlCommand
import org.snappy.statement.StatementType
import java.sql.Connection

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
): T? = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .querySingleOrNull(this)

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
): T = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .querySingle(this)

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
): T? = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .querySingleOrNullSuspend(this)

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
): T = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .querySingleSuspend(this)

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
): T? = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .queryFirstOrNull(this)

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
): T = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .queryFirst(this)

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
): T? = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .queryFirstOrNullSuspend(this)

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
): T = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .queryFirstSuspend(this)
