package org.snappy.extensions

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.ParameterBatch
import org.snappy.SqlParameter
import org.snappy.StatementType
import java.lang.IllegalStateException
import java.sql.Connection
import java.sql.PreparedStatement

/**
 * Create a new [PreparedStatement] using the provided details. If the [statementType] is
 * [StatementType.StoredProcedure] the [sql] query is intended to be a procedure name that is
 * transformed into a JDBC stored procedure call.
 *
 * If anything within the function throws an exception the statement will always be closed before
 * the exception is rethrown.
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException statement is null before returning (should never happen)
 */
internal fun <T : ParameterBatch> Connection.getBatchedStatement(
    sql: String,
    batchedParameters: List<T>,
    statementType: StatementType,
    timeout: UInt?,
): PreparedStatement {
    check(!isClosed) { "Cannot query a closed connection" }
    var statement: PreparedStatement? = null
    try {
        statement = when (statementType) {
            StatementType.StoredProcedure -> prepareCall(
                "{call $sql(${"?,".repeat(batchedParameters.size).trim(',')})"
            )
            StatementType.Text -> prepareStatement(sql)
        }
        statement?.let {
            timeout?.let {
                statement.queryTimeout = it.toInt()
            }
            for (batch in batchedParameters) {
                for ((i, parameter) in batch.toParameterBatch().withIndex()) {
                    statement.setParameter(i + 1, SqlParameter.In(parameter))
                }
                statement.addBatch()
            }
        }
        return statement
            ?: throw IllegalStateException("Statement cannot be null when exiting function")
    } catch (t: Throwable) {
        statement?.close()
        throw t
    }
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 */
fun <T : ParameterBatch> Connection.executeBatch(
    sql: String,
    batchedParameters: List<T> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): IntArray {
    if (batchedParameters.isEmpty()) {
        return intArrayOf(0)
    }
    return getBatchedStatement(
        sql,
        batchedParameters,
        statementType,
        timeout,
    ).use { preparedStatement ->
        preparedStatement.executeBatch()
    }
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * Suspends a call to [execute] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 */
suspend fun <T : ParameterBatch> Connection.executeBatchSuspend(
    sql: String,
    batchedParameters: List<T> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): IntArray = withContext(Dispatchers.IO) {
    executeBatch(sql, batchedParameters, statementType, timeout)
}


/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This method call should be used if the number of rows affected might exceed [Int.MAX_VALUE].
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 */
fun <T : ParameterBatch> Connection.executeBatchLarge(
    sql: String,
    batchedParameters: List<T> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): LongArray {
    if (batchedParameters.isEmpty()) {
        return longArrayOf(0)
    }
    return getBatchedStatement(
        sql,
        batchedParameters,
        statementType,
        timeout,
    ).use { preparedStatement ->
        preparedStatement.executeLargeBatch()
    }
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This method call should be used if the number of rows affected might exceed [Int.MAX_VALUE].
 * Suspends a call to [execute] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 */
suspend fun <T : ParameterBatch> Connection.executeBatchLargeSuspend(
    sql: String,
    batchedParameters: List<T> = emptyList(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): LongArray = withContext(Dispatchers.IO) {
    executeBatchLarge(sql, batchedParameters, statementType, timeout)
}
