package org.snappy.query

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.command.sqlCommand
import org.snappy.statement.StatementType
import java.sql.Connection

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
 * @see java.sql.Statement.executeQuery
 */
inline fun <reified T : Any> Connection.query(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): Sequence<T> = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .query(this)

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
 * @see java.sql.Statement.executeQuery
 * @see withContext
 * @see Dispatchers.IO
 */
suspend inline fun <reified T : Any> Connection.querySuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): Sequence<T> = sqlCommand(sql, statementType, timeout)
    .bindMany(parameters)
    .querySuspend(this)
