package org.snappy.query

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.snappy.EmptyResult
import org.snappy.SnappyMapper
import org.snappy.decode.Decoder
import org.snappy.statement.StatementType
import org.snappy.extensions.columnNames
import org.snappy.extensions.getStatement
import org.snappy.rowparse.SnappyRowImpl
import java.sql.Connection
import kotlin.reflect.KClass
import kotlin.reflect.full.createType

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
    sql: String,
    parameters: List<Any>,
    statementType: StatementType,
    timeout: UInt?,
    scalarValueClass: KClass<T>
): T? {
    return connection.getStatement(sql, parameters, statementType, timeout).use { preparedStatement ->
        preparedStatement.executeQuery().use { rs ->
            if (rs.next()) {
                val row = SnappyRowImpl(rs, rs.columnNames)
                val decoder = SnappyMapper.decoderCache
                    .getOrThrow(scalarValueClass.createType(nullable = false)) as Decoder<T>
                decoder.decodeNullable(row, rs.metaData.getColumnName(1))
            } else {
                null
            }
        }
    }
}

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
): T? {
    return queryScalarImpl(this, sql, parameters, statementType, timeout, T::class)
}

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
): T? = withContext(Dispatchers.IO) {
    queryScalarOrNull(sql, parameters, statementType, timeout)
}

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
): T {
    return queryScalarImpl(this, sql, parameters, statementType, timeout, T::class)
        ?: throw EmptyResult()
}

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
): T = withContext(Dispatchers.IO) {
    queryScalar(sql, parameters, statementType, timeout)
}
