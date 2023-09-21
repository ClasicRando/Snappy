package org.snappy.result

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.EmptyResult
import org.snappy.NoMoreResults
import org.snappy.SnappyMapper
import org.snappy.TooManyRows
import org.snappy.extensions.columnNames
import org.snappy.rowparse.SnappyRowImpl
import java.sql.ResultSet
import java.sql.Statement

/**
 * Container for a [Statement] that is expected to return multiple [java.sql.ResultSet]s.
 *
 * To get the next [Sequence] of results from the [statement], call the appropriate `readNext`
 * method. Usually you will know ahead of time how many result you expect. However, if you are
 * unsure how many results a query will return, call this method until you encounter a
 * [NoMoreResults] exception.
 */
class MultiResult(private val statement: Statement) : AutoCloseable {
    @PublishedApi
    internal var resultSet: ResultSet? = null
    /**
     * Move the [statement] to the next result, returning the next [ResultSet]
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     */
    @PublishedApi
    internal fun moveNextResult() {
        check(!statement.isClosed) { "Cannot read ResultSet from closed statement" }
        if (resultSet != null && !statement.moreResults) {
            throw NoMoreResults()
        }
        resultSet = statement.resultSet ?: throw NoMoreResults()
    }

    /**
     * Blocking call to read the next [java.sql.ResultSet] from [statement] and parse the result as
     * rows of the generic type [T].
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     */
    inline fun <reified T : Any> readNext(): Sequence<T> = sequence {
        moveNextResult()
        checkNotNull(resultSet)
        val rowParser = SnappyMapper.rowParserCache.getOrDefault<T>()
        resultSet?.use { rs ->
            val columnNames = rs.columnNames
            while (rs.next()) {
                val row = SnappyRowImpl(rs, columnNames)
                yield(rowParser.parseRow(row))
            }
        }
    }.constrainOnce()

    /**
     * Suspending call to read the next [java.sql.ResultSet] from [statement] and parse the result
     * as rows of the generic type [T].
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     */
    suspend inline fun <reified T : Any> readNextSuspend(): Sequence<T> {
        return withContext(Dispatchers.IO) { this@MultiResult.readNext() }
    }

    /**
     * Blocking call to read the next [java.sql.ResultSet] from [statement] and parse the result as
     * a single row of the generic type [T] or null if no rows are returned.
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     * - more than 1 row is returned as the result
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     * @exception TooManyRows multiple rows are returned from the result
     */
    inline fun <reified T : Any> readNextSingleOrNull(): T? {
        moveNextResult()
        checkNotNull(resultSet)
        val rowParser = SnappyMapper.rowParserCache.getOrDefault<T>()
        return resultSet?.use { rs ->
            if (rs.next()) {
                val row = SnappyRowImpl(rs, rs.columnNames)
                val mappedRow = rowParser.parseRow(row)
                if (rs.next()) {
                    throw TooManyRows()
                }
                mappedRow
            } else {
                null
            }
        }
    }

    /**
     * Blocking call to read the next [java.sql.ResultSet] from [statement] and parse the result as
     * a single row of the generic type [T].
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     * - the result is empty
     * - more than 1 row is returned as the result
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     * @exception TooManyRows multiple rows are returned from the result
     * @exception EmptyResult result is empty
     */
    inline fun <reified T : Any> readNextSingle(): T {
        return readNextSingleOrNull() ?: throw EmptyResult()
    }

    /**
     * Suspending call to read the next [java.sql.ResultSet] from [statement] and parse the result
     * as a single row of the generic type [T] or null if no rows are returned.
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     * - more than 1 row is returned as the result
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     * @exception TooManyRows multiple rows are returned from the result
     */
    suspend inline fun <reified T : Any> readNextSingleOrNullSuspend(): T? {
        return withContext(Dispatchers.IO) { this@MultiResult.readNextSingleOrNull() }
    }

    /**
     * Suspending call to read the next [java.sql.ResultSet] from [statement] and parse the result
     * as a single row of the generic type [T].
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     * - the result is empty
     * - more than 1 row is returned as the result
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     * @exception TooManyRows multiple rows are returned from the result
     * @exception EmptyResult result is empty
     */
    suspend inline fun <reified T : Any> readNextSingleSuspend(): T {
        return withContext(Dispatchers.IO) { this@MultiResult.readNextSingle() }
    }

    /**
     * Blocking call to read the next [java.sql.ResultSet] from [statement] and parse the first row
     * of the result as a row of the generic type [T] or null if no rows are returned.
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     */
    inline fun <reified T : Any> readNextFirstOrNull(): T? {
        moveNextResult()
        checkNotNull(resultSet)
        val rowParser = SnappyMapper.rowParserCache.getOrDefault<T>()
        return resultSet?.use { rs ->
            if (rs.next()) {
                val row = SnappyRowImpl(rs, rs.columnNames)
                val mappedRow = rowParser.parseRow(row)
                mappedRow
            } else {
                null
            }
        }
    }

    /**
     * Blocking call to read the next [java.sql.ResultSet] from [statement] and parse the first row
     * of the result as a row of the generic type [T].
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     * - the result is empty
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     * @exception EmptyResult result is empty
     */
    inline fun <reified T : Any> readNextFirst(): T {
        return readNextFirstOrNull() ?: throw EmptyResult()
    }

    /**
     * Suspending call to read the next [java.sql.ResultSet] from [statement] and parse the first
     * row of the result as a row of the generic type [T] or null if no rows are returned.
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     */
    suspend inline fun <reified T : Any> readNextFirstOrNullSuspend(): T? {
        return withContext(Dispatchers.IO) { this@MultiResult.readNextFirstOrNull() }
    }

    /**
     * Suspending call to read the next [java.sql.ResultSet] from [statement] and parse the first
     * row of the result as a row of the generic type [T].
     *
     * This call can fail if:
     * - the [statement] is closed
     * - there are no more result sets to parse
     * - mapping the [java.sql.ResultSet] fails
     * - the result is empty
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception NoMoreResults results have been exhausted when called
     * @exception IllegalStateException [statement] is closed when called
     * @exception EmptyResult result is empty
     */
    suspend inline fun <reified T : Any> readNextFirstSuspend(): T? {
        return withContext(Dispatchers.IO) { this@MultiResult.readNextFirstOrNull() }
    }

    override fun close() {
        statement.close()
    }
}