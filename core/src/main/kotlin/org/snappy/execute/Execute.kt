package org.snappy.execute

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.command.sqlCommand
import org.snappy.statement.StatementType
import java.sql.Connection

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.execute
 * @see java.sql.Statement.getUpdateCount
 */
fun Connection.execute(
    sql: String,
    parameters: List<Any?> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): Int = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .execute(this)

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * Suspends a call to [execute] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.execute
 * @see java.sql.Statement.getUpdateCount
 * @see withContext
 * @see Dispatchers.IO
 */
suspend fun Connection.executeSuspend(
    sql: String,
    parameters: List<Any?> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): Int = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .executeSuspend(this)


/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This method call should be used if the number of rows affected might exceed [Int.MAX_VALUE].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.execute
 * @see java.sql.Statement.getLargeUpdateCount
 */
fun Connection.executeLarge(
    sql: String,
    parameters: List<Any?> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): Long = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .executeLarge(this)

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This method call should be used if the number of rows affected might exceed [Int.MAX_VALUE].
 * Suspends a call to [execute] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.execute
 * @see java.sql.Statement.getLargeUpdateCount
 * @see withContext
 * @see Dispatchers.IO
 */
suspend fun Connection.executeLargeSuspend(
    sql: String,
    parameters: List<Any?> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): Long = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .executeLargeSuspend(this)

/**
 * Execute a stored procedure with `OUT` parameters against this [Connection], returning a [List]
 * of OUTPUT parameters values returned in the order of the [parameters] provided
 *
 * @param procedureName query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.execute
 * @see java.sql.CallableStatement
 */
fun Connection.executeOutParameters(
    procedureName: String,
    parameters: List<Any?>,
    timeout: UInt? = null,
): List<Any?> = sqlCommand(procedureName, StatementType.StoredProcedure, timeout)
    .bindMany(parameters)
    .executeOutParameters(this)

/**
 * Execute a stored procedure with `OUT` parameters against this [Connection], returning a [List]
 * of OUTPUT parameters values returned in the order of the [parameters] provided. Suspends a call
 * to [executeOutParameters] within the context of [Dispatchers.IO].
 *
 * @param procedureName query or procedure name to execute
 * @param parameters values to add to the [java.sql.PreparedStatement], default is no parameters
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.execute
 * @see java.sql.CallableStatement
 * @see withContext
 * @see Dispatchers.IO
 */
suspend fun Connection.executeOutParametersSuspend(
    procedureName: String,
    parameters: List<Any?>,
    timeout: UInt? = null,
): List<Any?> = sqlCommand(procedureName, StatementType.StoredProcedure, timeout)
    .bindMany(parameters)
    .executeOutParametersSuspend(this)
