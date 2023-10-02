package org.snappy.command

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.EmptyResult
import org.snappy.SnappyMapper
import org.snappy.TooManyRows
import org.snappy.decode.Decoder
import org.snappy.execute.execute
import org.snappy.execute.executeOutParameters
import org.snappy.extensions.columnNames
import org.snappy.extensions.getStatement
import org.snappy.query.RowReturn
import org.snappy.result.MultiResult
import org.snappy.rowparse.RowParser
import org.snappy.rowparse.SnappyRowImpl
import org.snappy.statement.SqlParameter
import org.snappy.statement.StatementType
import java.sql.CallableStatement
import java.sql.Connection
import kotlin.reflect.KType
import kotlin.reflect.typeOf

fun sqlCommand(
    sql: String,
    statementType: StatementType = StatementType.Text,
    timeout: UInt? = null,
): SqlCommand = SqlCommand(sql, statementType, timeout)

class SqlCommand internal constructor(
    internal val sql: String,
    internal val statementType: StatementType,
    internal val timeout: UInt?,
) {
    internal val commandParameters = mutableListOf<SqlParameter>()

    fun <T> bind(parameter: T): SqlCommand {
        if (parameter is SqlParameter) {
            commandParameters.add(parameter)
            return this
        }
        commandParameters.add(SqlParameter.In(parameter))
        return this
    }

    fun <T> bindMany(vararg parameters: Array<T>): SqlCommand {
        return bindMany(parameters.asIterable())
    }

    fun <T> bindMany(parameters: Array<T>): SqlCommand {
        return bindMany(parameters.asIterable())
    }

    fun <T> bindMany(parameters: Iterable<T>): SqlCommand {
        for (parameter in parameters) {
            bind(parameter)
        }
        return this
    }

    /**
     * Implementation of querying a connection for a [Sequence] of [T] rows. Prepares a statement
     * using the provided parameters, yielding [java.sql.ResultSet] rows, parsed using [rowParser]
     * into the required type [T]. Note, this [Sequence] wraps the statement and result parsing to
     * both resources are closed when an error occurs.
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     */
    @PublishedApi
    internal fun <T> queryImpl(
        connection: Connection,
        rowParser: RowParser<T>,
    ): Sequence<T> = sequence {
        connection.getStatement(this@SqlCommand).use { preparedStatement ->
            preparedStatement.executeQuery().use { rs ->
                val columnNames = rs.columnNames
                while (rs.next()) {
                    val row = SnappyRowImpl(rs, columnNames)
                    yield(rowParser.parseRow(row))
                }
            }
        }
    }.constrainOnce()

    /**
     * Execute this [SqlCommand] against the [connection], yielding rows from the result as a
     * [Sequence] of [T].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.executeQuery
     */
    inline fun <reified T : Any> query(connection: Connection): Sequence<T> {
        val rowParser = SnappyMapper.rowParserCache.getOrThrow<T>()
        return queryImpl(connection, rowParser)
    }

    /**
     * Execute this [SqlCommand] against the [connection], yielding rows from the result as a
     * [Sequence] of [T]. Suspends a call to [query] within the context of [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.executeQuery
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend inline fun <reified T : Any> querySuspend(connection: Connection): Sequence<T> {
        return withContext(Dispatchers.IO) { query(connection) }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning a reader for multiple results.
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.execute
     * @see java.sql.Statement.getResultSet
     * @see java.sql.Statement.getMoreResults
     */
    fun queryMultiple(connection: Connection): MultiResult {
        val statement = connection.getStatement(this)
        statement.execute()
        return MultiResult(statement)
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning a reader for multiple results.
     * Suspends a call to [queryMultiple] within the context of [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.execute
     * @see java.sql.Statement.getResultSet
     * @see java.sql.Statement.getMoreResults
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend fun queryMultipleSuspend(connection: Connection): MultiResult {
        return withContext(Dispatchers.IO) { queryMultiple(connection) }
    }

    /**
     * Implementation of querying a connection for a single row. Prepares a statement using the
     * provided parameters, reading the result and parsing the row using [rowParser] into the
     * required type [T]. Note, both the statement and result set are closed by default before
     * returning from the function.
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception TooManyRows if [rowReturn] is [RowReturn.Single] and more than 1 row is returned
     */
    @PublishedApi
    internal fun <T> querySingleRowImpl(
        connection: Connection,
        rowParser: RowParser<T>,
        rowReturn: RowReturn,
    ): T? {
        return connection.getStatement(this).use { preparedStatement ->
            preparedStatement.executeQuery().use { rs ->
                if (rs.next()) {
                    val row = SnappyRowImpl(rs, rs.columnNames)
                    val mappedRow = rowParser.parseRow(row)
                    if (rowReturn == RowReturn.Single && rs.next()) {
                        throw TooManyRows()
                    }
                    mappedRow
                } else {
                    null
                }
            }
        }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning at most a single row of [T]
     * (null if no rows returned)
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception TooManyRows result contains more than 1 row
     * @see java.sql.Statement.executeQuery
     */
    inline fun <reified T : Any> querySingleOrNull(connection: Connection): T? {
        val rowParser = SnappyMapper.rowParserCache.getOrThrow<T>()
        return querySingleRowImpl(connection, rowParser, RowReturn.Single)
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning a single row of [T]
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception TooManyRows result contains more than 1 row
     * @exception EmptyResult result contain no rows
     * @see java.sql.Statement.executeQuery
     */
    inline fun <reified T : Any> querySingle(connection: Connection): T {
        return querySingleOrNull<T>(connection) ?: throw EmptyResult()
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning at most a single row of [T]
     * (null if no rows returned). Suspends a call to [querySingleOrNull] within the context of
     * [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception TooManyRows result contains more than 1 row
     * @see java.sql.Statement.executeQuery
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend inline fun <reified T : Any> querySingleOrNullSuspend(connection: Connection): T? {
        return withContext(Dispatchers.IO) { querySingleOrNull(connection) }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning a single row of [T]. Suspends a
     * call to [querySingle] within the context of [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception TooManyRows result contains more than 1 row
     * @exception EmptyResult result contain no rows
     * @see java.sql.Statement.executeQuery
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend inline fun <reified T : Any> querySingleSuspend(connection: Connection): T {
        return withContext(Dispatchers.IO) { querySingle(connection) }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the first row of [T] (null if
     * no rows returned)
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.executeQuery
     */
    inline fun <reified T : Any> queryFirstOrNull(connection: Connection): T? {
        val rowParser = SnappyMapper.rowParserCache.getOrThrow<T>()
        return querySingleRowImpl(connection, rowParser, RowReturn.First)
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the first row of [T].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception EmptyResult result contain no rows
     * @see java.sql.Statement.executeQuery
     */
    inline fun <reified T : Any> queryFirst(connection: Connection): T {
        return queryFirstOrNull(connection) ?: throw EmptyResult()
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the first row of [T] (null if
     * no rows returned). Suspends a call to [queryFirstOrNull] within the context of
     * [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.executeQuery
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend inline fun <reified T : Any> queryFirstOrNullSuspend(connection: Connection): T? {
        return withContext(Dispatchers.IO) { queryFirstOrNull(connection) }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the first row of [T].. Suspends
     * a call to [queryFirst] within the context of [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception EmptyResult result contain no rows
     * @see java.sql.Statement.executeQuery
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend inline fun <reified T : Any> queryFirstSuspend(connection: Connection): T {
        return withContext(Dispatchers.IO) { queryFirst(connection) }
    }

    /**
     * Implementation of querying a connection for a single value. Prepares a statement using the
     * provided parameters, reading the result and parsing the first row and first column into the
     * required type [T]. Note, both the statement and result set are closed by default before returning
     * from the function.
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     */
    @Suppress("UNCHECKED_CAST")
    @PublishedApi
    internal fun <T : Any> queryScalarImpl(
        connection: Connection,
        scalarValueType: KType
    ): T? {
        return connection.getStatement(this).use { preparedStatement ->
            preparedStatement.executeQuery().use { rs ->
                if (rs.next()) {
                    val row = SnappyRowImpl(rs, rs.columnNames)
                    val decoder = SnappyMapper.decoderCache
                        .getOrThrow(scalarValueType) as Decoder<T>
                    decoder.decodeNullable(row, rs.metaData.getColumnName(1))
                } else {
                    null
                }
            }
        }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the first row and column as the
     * type [T] (null if no rows are returned or the value is null).
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.executeQuery
     */
    inline fun <reified T : Any> queryScalarOrNull(connection: Connection): T? {
        return queryScalarImpl(connection, typeOf<T>())
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the first row and column as the
     * type [T] (null if no rows are returned or the value is null). Suspends a call to
     * [queryScalarOrNull] within the context of [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.executeQuery
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend inline fun <reified T : Any> queryScalarOrNullSuspend(connection: Connection): T? {
        return withContext(Dispatchers.IO) { queryScalarOrNull(connection) }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the first row and column as the
     * type [T].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception EmptyResult result contain no rows
     * @see java.sql.Statement.executeQuery
     */
    inline fun <reified T : Any> queryScalar(connection: Connection): T {
        return queryScalarOrNull(connection) ?: throw EmptyResult()
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the first row and column as the
     * type [T]. Suspends a call to [queryScalar] within the context of [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @exception EmptyResult result contain no rows
     * @see java.sql.Statement.executeQuery
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend inline fun <reified T : Any> queryScalarSuspend(connection: Connection): T {
        return withContext(Dispatchers.IO) { queryScalar(connection) }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the number of rows affected by
     * the query.
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.execute
     * @see java.sql.Statement.getUpdateCount
     */
    fun execute(connection: Connection): Int {
        check(!connection.isClosed) { "Cannot query a closed connection" }
        return connection.getStatement(this).use { preparedStatement ->
            preparedStatement.execute()
            preparedStatement.updateCount
        }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the number of rows affected by
     * the query. Suspends a call to [execute] within the context of [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.execute
     * @see java.sql.Statement.getUpdateCount
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend fun executeSuspend(connection: Connection): Int {
        return withContext(Dispatchers.IO) { execute(connection) }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the number of rows affected by
     * the query. This method call should be used if the number of rows affected might exceed
     * [Int.MAX_VALUE].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.execute
     * @see java.sql.Statement.getUpdateCount
     */
    fun executeLarge(connection: Connection): Long {
        check(!connection.isClosed) { "Cannot query a closed connection" }
        return connection.getStatement(this).use { preparedStatement ->
            preparedStatement.execute()
            preparedStatement.largeUpdateCount
        }
    }

    /**
     * Execute this [SqlCommand] against the [connection], returning the number of rows affected by
     * the query. This method call should be used if the number of rows affected might exceed
     * [Int.MAX_VALUE]. Suspends a call to [execute] within the context of [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.execute
     * @see java.sql.Statement.getUpdateCount
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend fun executeLargeSuspend(connection: Connection): Long {
        return withContext(Dispatchers.IO) { executeLarge(connection) }
    }

    /**
     * Execute a stored procedure with `OUT` parameters against the [connection], returning a [List]
     * of OUTPUT parameters values returned in the order of the parameters bound to the
     * [SqlCommand].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.execute
     * @see java.sql.CallableStatement
     */
    fun executeOutParameters(connection: Connection): List<Any?> {
        check(!connection.isClosed) { "Cannot query a closed connection" }
        check(statementType == StatementType.StoredProcedure) {
            "Cannot call this method when the statement type is not StoredProcedure"
        }
        return connection.getStatement(this).use { callableStatement ->
            callableStatement.execute()
            callableStatement as CallableStatement
            commandParameters.mapIndexedNotNull { index, parameter ->
                if (parameter !is SqlParameter.Out) {
                    return@mapIndexedNotNull null
                }
                callableStatement.getObject(index + 1)
            }
        }
    }

    /**
     * Execute a stored procedure with `OUT` parameters against the [connection], returning a [List]
     * of OUTPUT parameters values returned in the order of the parameters bound to the
     * [SqlCommand]. Suspends a call to [executeOutParameters] within the context of
     * [Dispatchers.IO].
     *
     * @exception java.sql.SQLException underlining database operation fails
     * @exception IllegalStateException the connection is closed
     * @see java.sql.Statement.execute
     * @see java.sql.CallableStatement
     * @see withContext
     * @see Dispatchers.IO
     */
    suspend fun executeOutParametersSuspend(connection: Connection): List<Any?> {
        return withContext(Dispatchers.IO) { executeOutParameters(connection) }
    }
}
