package org.snappy.batch

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.BatchExecutionFailed
import org.snappy.statement.StatementType
import org.snappy.extensions.chunkedIter
import org.snappy.extensions.getStatement
import org.snappy.extensions.setParameter
import java.sql.Connection

private const val EXECUTE_FAILED_LONG = java.sql.Statement.EXECUTE_FAILED.toLong()

/** Return the [batchSize] provided or 100 if the value is null or equal to zero */
internal fun batchSizeOrDefault(batchSize: UInt?): Int {
    return batchSize?.toInt()?.takeIf { it > 0 } ?: 100
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
    batchedParameters: Sequence<T> = emptySequence(),
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
): IntArray {
    val finalBatchSize = batchSizeOrDefault(batchSize)
    return getStatement(
        sql,
        emptyList(),
        statementType,
        timeout,
    ).use { preparedStatement ->
        batchedParameters.chunkedIter(finalBatchSize).fold(intArrayOf()) { acc, batchedParameter ->
            var batchNumber = 0u
            for (batch in batchedParameter) {
                for ((i, parameter) in batch.toSqlParameterBatch().withIndex()) {
                    preparedStatement.setParameter(i + 1, parameter)
                }
                preparedStatement.addBatch()
                batchNumber++
            }
            val result = preparedStatement.executeBatch()
            if (failOnErrorReturn && result.any { it == java.sql.Statement.EXECUTE_FAILED }) {
                throw BatchExecutionFailed(sql, batchNumber)
            }
            acc + result
        }
    }
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
 * @param transaction flag indicating that the operation should be wrapped in a transaction for
 * consistency (i.e. all batches are successful or no batches are successful), default is false
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
    transaction: Boolean = false,
): IntArray = withContext(Dispatchers.IO) {
    executeBatch(sql, batchedParameters, statementType, timeout, batchSize, transaction)
}

/**
 * Execute a query with batches, expecting batches to return a rows impacted number that is greater
 * than [Int.MAX_VALUE]. Creates a [java.sql.PreparedStatement], executing each batch of parameters
 * against the statement, sending chunks of the batches based upon the [batchSize].
 */
internal fun <T : ParameterBatch> executeLargeBatch(
    connection: Connection,
    sql: String,
    batchedParameters: Sequence<T>,
    statementType: StatementType,
    timeout: UInt?,
    batchSize: Int,
    failOnErrorReturn: Boolean,
) : LongArray {
    return connection.getStatement(
        sql,
        emptyList(),
        statementType,
        timeout,
    ).use { preparedStatement ->
        batchedParameters.chunked(batchSize).fold(longArrayOf()) { acc, batchedParameter ->
            var batchNumber = 0u
            for (batch in batchedParameter) {
                for ((i, parameter) in batch.toSqlParameterBatch().withIndex()) {
                    preparedStatement.setParameter(i + 1, parameter)
                }
                preparedStatement.addBatch()
                batchNumber++
            }
            val result = preparedStatement.executeLargeBatch()
            if (failOnErrorReturn && result.any { it == EXECUTE_FAILED_LONG }) {
                throw BatchExecutionFailed(sql, batchNumber)
            }
            acc + result
        }
    }
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
    val finalBatchSize = batchSizeOrDefault(batchSize)
    return getStatement(
        sql,
        emptyList(),
        statementType,
        timeout,
    ).use { preparedStatement ->
        batchedParameters.chunked(finalBatchSize).fold(longArrayOf()) { acc, batchedParameter ->
            var batchNumber = 0u
            for (batch in batchedParameter) {
                for ((i, parameter) in batch.toSqlParameterBatch().withIndex()) {
                    preparedStatement.setParameter(i + 1, parameter)
                }
                preparedStatement.addBatch()
                batchNumber++
            }
            val result = preparedStatement.executeLargeBatch()
            if (failOnErrorReturn && result.any { it == EXECUTE_FAILED_LONG }) {
                throw BatchExecutionFailed(sql, batchNumber)
            }
            acc + result
        }
    }
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
 * @param transaction flag indicating that the operation should be wrapped in a transaction for
 * consistency (i.e. all batches are successful or no batches are successful), default is false
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
    transaction: Boolean = false,
): LongArray = withContext(Dispatchers.IO) {
    executeLargeBatch(sql, batchedParameters, statementType, timeout, batchSize, transaction)
}
