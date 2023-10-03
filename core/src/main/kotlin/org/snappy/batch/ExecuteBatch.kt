package org.snappy.batch

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.BatchExecutionFailed
import org.snappy.command.batchSqlCommand
import org.snappy.statement.StatementType
import java.sql.Connection

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 * @param batchSize size of batches to send to the database for processing, default is 100
 * @param failOnErrorReturn flag indicating that the operation should throw a [BatchExecutionFailed]
 * when the records affected count is [java.sql.Statement.EXECUTE_FAILED]
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeBatch
 */
fun <T : ParameterBatch> Connection.executeBatch(
    sql: String,
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
    batchedParameters: suspend SequenceScope<T>.() -> Unit,
): IntArray {
    return batchSqlCommand(sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        sequence(batchedParameters)
    ).execute(this)
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 * @param batchSize size of batches to send to the database for processing, default is 100
 * @param failOnErrorReturn flag indicating that the operation should throw a [BatchExecutionFailed]
 * when the records affected count is [java.sql.Statement.EXECUTE_FAILED]
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeBatch
 */
fun <T : ParameterBatch> Connection.executeBatch(
    sql: String,
    batchedParameters: Sequence<T>,
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
): IntArray {
    return batchSqlCommand(sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        batchedParameters,
    ).execute(this)
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 * @param batchSize size of batches to send to the database for processing, default is 100
 * @param failOnErrorReturn flag indicating that the operation should throw a [BatchExecutionFailed]
 * when the records affected count is [java.sql.Statement.EXECUTE_FAILED]
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeBatch
 */
suspend fun <T : ParameterBatch> Connection.executeBatchSuspend(
    sql: String,
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
    batchedParameters: suspend SequenceScope<T>.() -> Unit,
): IntArray {
    return batchSqlCommand(sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        batchedParameters,
    ).executeSuspend(this)
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This operation is wrapped in a transaction to force the operation to be completed in its entirety
 * or not at all. Suspends a call to [executeBatch] within the context of [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 * @param batchSize size of batches to send to the database for processing, default is 100
 * @param failOnErrorReturn flag indicating that the operation should throw a [BatchExecutionFailed]
 * when the records affected count is [java.sql.Statement.EXECUTE_FAILED]
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeBatch
 * @see withContext
 * @see Dispatchers.IO
 */
suspend fun <T : ParameterBatch> Connection.executeBatchSuspend(
    sql: String,
    batchedParameters: Sequence<T> = emptySequence(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
): IntArray {
    return batchSqlCommand(sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        batchedParameters,
    ).executeSuspend(this)
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This operation is wrapped in a transaction to force the operation to be completed in its entirety
 * or not at all. This method call should be used if the number of rows affected by any batch might
 * exceed [Int.MAX_VALUE].
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 * @param batchSize size of batches to send to the database for processing, default is 100
 * @param failOnErrorReturn flag indicating that the operation should throw a [BatchExecutionFailed]
 * when the records affected count is [java.sql.Statement.EXECUTE_FAILED]
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeLargeBatch
 */
fun <T : ParameterBatch> Connection.executeLargeBatch(
    sql: String,
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
    batchedParameters: SequenceScope<T>.() -> Unit,
): LongArray {
    return batchSqlCommand(sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        batchedParameters,
    ).executeLarge(this)
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This operation is wrapped in a transaction to force the operation to be completed in its entirety
 * or not at all. This method call should be used if the number of rows affected by any batch might
 * exceed [Int.MAX_VALUE].
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 * @param batchSize size of batches to send to the database for processing, default is 100
 * @param failOnErrorReturn flag indicating that the operation should throw a [BatchExecutionFailed]
 * when the records affected count is [java.sql.Statement.EXECUTE_FAILED]
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeLargeBatch
 */
fun <T : ParameterBatch> Connection.executeLargeBatch(
    sql: String,
    batchedParameters: Sequence<T> = emptySequence(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
): LongArray {
    return batchSqlCommand(sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        batchedParameters,
    ).executeLarge(this)
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This operation is wrapped in a transaction to force the operation to be completed in its entirety
 * or not at all. This method call should be used if the number of rows affected by any batch might
 * exceed [Int.MAX_VALUE]. Suspends a call to [executeLargeBatch] within the context of
 * [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 * @param batchSize size of batches to send to the database for processing, default is 100
 * @param failOnErrorReturn flag indicating that the operation should throw a [BatchExecutionFailed]
 * when the records affected count is [java.sql.Statement.EXECUTE_FAILED]
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeLargeBatch
 * @see withContext
 * @see Dispatchers.IO
 */
suspend fun <T : ParameterBatch> Connection.executeLargeBatchSuspend(
    sql: String,
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
    batchedParameters: SequenceScope<T>.() -> Unit,
): LongArray {
    return batchSqlCommand(sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        batchedParameters,
    ).executeLargeSuspend(this)
}

/**
 * Execute a query against this [Connection], returning the number of rows affected by the query.
 * This operation is wrapped in a transaction to force the operation to be completed in its entirety
 * or not at all. This method call should be used if the number of rows affected by any batch might
 * exceed [Int.MAX_VALUE]. Suspends a call to [executeLargeBatch] within the context of
 * [Dispatchers.IO].
 *
 * @param sql query or procedure name to execute
 * @param batchedParameters
 * @param statementType query variant as [StatementType.Text] (default) or
 * [StatementType.StoredProcedure]
 * @param timeout query timeout in seconds, default is unlimited time
 * @param batchSize size of batches to send to the database for processing, default is 100
 * @param failOnErrorReturn flag indicating that the operation should throw a [BatchExecutionFailed]
 * when the records affected count is [java.sql.Statement.EXECUTE_FAILED]
 *
 * @exception java.sql.SQLException underlining database operation fails
 * @exception IllegalStateException the connection is closed
 * @see java.sql.Statement.executeLargeBatch
 * @see withContext
 * @see Dispatchers.IO
 */
suspend fun <T : ParameterBatch> Connection.executeLargeBatchSuspend(
    sql: String,
    batchedParameters: Sequence<T> = emptySequence(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
): LongArray {
    return batchSqlCommand(sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        batchedParameters,
    ).executeLargeSuspend(this)
}
