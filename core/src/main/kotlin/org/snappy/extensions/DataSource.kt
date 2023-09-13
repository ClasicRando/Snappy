package org.snappy.extensions

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import javax.sql.DataSource

/**
 * Convenience method to execute a [block] within the context of a [Connection] from a [DataSource].
 * [Connection] used within [block] will always be closed with exceptions rethrown.
 *
 * @see DataSource.getConnection
 * @see AutoCloseable.use
 */
inline fun <T> DataSource.useConnection(block: Connection.() -> T): T = this.connection.use(block)

/**
 * Convenience method to execute a [block] within the context of a [Connection] from a [DataSource].
 * [Connection] used within [block] will always be closed with exceptions rethrown. Suspends while
 * executing [block] within the context of [Dispatchers.IO].
 *
 * @see withContext
 * @see Dispatchers.IO
 * @see DataSource.getConnection
 * @see AutoCloseable.use
 */
suspend inline fun <T> DataSource.useConnectionSuspend(
    crossinline block: suspend Connection.() -> T,
): T = withContext(Dispatchers.IO) {
    connection.use { block(it) }
}

/**
 * Convenience method to execute a [block] within the context of a [Connection] in transaction mode
 * from a [DataSource]. [Connection] used within [block] will always be closed with exceptions
 * rethrown and the transaction will be rolled back.
 *
 * @see DataSource.getConnection
 * @see AutoCloseable.use
 */
inline fun <T> DataSource.useTransaction(block: Connection.() -> T): T {
    return this.connection.asTransaction(block)
}

/**
 * Convenience method to execute a [block] within the context of a [Connection] in transaction mode
 * from a [DataSource]. [Connection] used within [block] will always be closed with exceptions
 * rethrown and the transaction will be rolled back. Suspends while executing [block] within the
 * context of [Dispatchers.IO].
 *
 * @see withContext
 * @see Dispatchers.IO
 * @see DataSource.getConnection
 * @see AutoCloseable.use
 */
suspend inline fun <T> DataSource.useTransaction(
    crossinline block: suspend Connection.() -> T,
): T = withContext(Dispatchers.IO) {
    this@useTransaction.connection.asTransactionSuspend(block)
}
