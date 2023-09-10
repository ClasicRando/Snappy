package org.snappy.extensions

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.MultiResult
import org.snappy.StatementType
import java.sql.Connection

/**
 * Execute a query against this [Connection], returning a reader for multiple results.
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
fun Connection.queryMultiple(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): MultiResult {
    val statement = getStatement(sql, parameters, statementType, timeout)
    statement.execute()
    return MultiResult(statement)
}

/**
 * Execute a query against this [Connection], returning a reader for multiple results. Suspends a
 * call to [queryMultiple] within the context of [Dispatchers.IO].
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
suspend inline fun Connection.queryMultipleSuspend(
    sql: String,
    parameters: List<Any> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): MultiResult = withContext(Dispatchers.IO) {
    queryMultiple(sql, parameters, statementType, timeout)
}
