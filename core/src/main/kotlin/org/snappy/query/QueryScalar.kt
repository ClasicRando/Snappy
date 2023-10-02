package org.snappy.query

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.EmptyResult
import org.snappy.command.sqlCommand
import org.snappy.statement.StatementType
import java.sql.Connection

/**
 * Execute a query against this [Connection], returning the first row and column as the type [T]
 * (null if no rows are returned).
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
inline fun <reified T : Any> Connection.queryScalarOrNull(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T? = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .queryScalarOrNull(this)

/**
 * Execute a query against this [Connection], returning the first row and column as the type [T]
 * (null if no rows are returned). Suspends a call to [queryScalarOrNull] within the context of
 * [Dispatchers.IO].
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
suspend inline fun <reified T : Any> Connection.queryScalarOrNullSuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T? = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .queryScalarOrNullSuspend(this)

/**
 * Execute a query against this [Connection], returning the first row and column as the type [T].
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
inline fun <reified T : Any> Connection.queryScalar(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .queryScalar(this)

/**
 * Execute a query against this [Connection], returning the first row and column as the type [T].
 * Suspends a call to [queryScalar] within the context of [Dispatchers.IO].
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
suspend inline fun <reified T : Any> Connection.queryScalarSuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): T = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .queryScalarSuspend(this)
