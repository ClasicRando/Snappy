package org.snappy.command

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.BatchExecutionFailed
import org.snappy.batch.ParameterBatch
import org.snappy.batch.toSqlParameterBatch
import org.snappy.extensions.chunkedIter
import org.snappy.extensions.getStatement
import org.snappy.extensions.setParameter
import org.snappy.statement.SqlParameter
import org.snappy.statement.StatementType
import java.sql.Connection

private const val EXECUTE_FAILED_LONG = java.sql.Statement.EXECUTE_FAILED.toLong()

/** Return the [batchSize] provided or 100 if the value is null or equal to zero */
internal fun batchSizeOrDefault(batchSize: UInt?): Int {
    return batchSize?.toInt()?.takeIf { it > 0 } ?: 100
}

fun <T : ParameterBatch> batchSqlCommand(
    sql: String,
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
    batchedParameters: suspend SequenceScope<T>.() -> Unit,
): BatchSqlCommand<T> {
    return BatchSqlCommand(
        sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        sequence(batchedParameters),
    )
}

fun <T : ParameterBatch> batchSqlCommand(
    sql: String,
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
    batchSize: UInt? = null,
    failOnErrorReturn: Boolean = false,
    batchedParameters: Sequence<T>,
): BatchSqlCommand<T> {
    return BatchSqlCommand(
        sql,
        statementType,
        timeout,
        batchSize,
        failOnErrorReturn,
        batchedParameters,
    )
}

class BatchSqlCommand<T : ParameterBatch> internal constructor(
    override val sql: String,
    override val statementType: StatementType,
    override val timeout: UInt?,
    private val batchSize: UInt?,
    private val failOnErrorReturn: Boolean,
    batchedParameters: Sequence<T>,
) : Command {
    private val batchIterator = batchedParameters.iterator()
    private val firstRow = if (batchIterator.hasNext()) {
        batchIterator.next().toSqlParameterBatch()
    } else null
    override val commandParameters: List<SqlParameter> = emptyList()
    override fun parameterCount(): Int = firstRow?.size ?: 0

    fun execute(connection: Connection): IntArray {
        val finalBatchSize = batchSizeOrDefault(batchSize)
        return connection.getStatement(this).use { preparedStatement ->
            if (firstRow == null) {
                return@use intArrayOf(0)
            }
            val fullSequence = sequence {
                yield(firstRow)
                yieldAll(batchIterator.asSequence().map { it.toSqlParameterBatch() })
            }
            fullSequence.chunkedIter(finalBatchSize).fold(intArrayOf()) { acc, batchedParameter ->
                var batchNumber = 0u
                for (batch in batchedParameter) {
                    for ((i, parameter) in batch.withIndex()) {
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

    suspend fun executeSuspend(connection: Connection): IntArray = withContext(Dispatchers.IO) {
        execute(connection)
    }

    fun executeLarge(connection: Connection): LongArray {
        val finalBatchSize = batchSizeOrDefault(batchSize)
        return connection.getStatement(this).use { preparedStatement ->
            if (firstRow == null) {
                return@use longArrayOf(0)
            }
            val fullSequence = sequence {
                yield(firstRow)
                yieldAll(batchIterator.asSequence().map { it.toSqlParameterBatch() })
            }
            fullSequence.chunkedIter(finalBatchSize).fold(longArrayOf()) { acc, batchedParameter ->
                var batchNumber = 0u
                for (batch in batchedParameter) {
                    for ((i, parameter) in batch.withIndex()) {
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

    suspend fun executeLargeSuspend(connection: Connection): LongArray = withContext(Dispatchers.IO) {
        executeLarge(connection)
    }
}
