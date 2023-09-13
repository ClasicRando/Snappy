package org.snappy.extensions

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection

/**
 * Execute the [block] action within the scope of a transaction. Regardless of the
 * [autoCommit][Connection.getAutoCommit] state of the [Connection], autoCommit is turned off while
 * the [block] executes. If no errors where thrown, [Connection.commit] is called and the result of
 * the [block] is returned. If an exception is thrown, [Connection.rollback] is called and the
 * exception is rethrown. In all cases, the [autoCommit][Connection.getAutoCommit] state of the
 * [Connection] is returned back to the original state.
 */
inline fun <T> Connection.asTransaction(block: Connection.() -> T): T {
    val preCommitMode = this.autoCommit
    return try {
        this.autoCommit = false
        val result = block(this)
        this.commit()
        result
    } catch (ex: Exception) {
        this.rollback()
        throw ex
    } finally {
        this.autoCommit = preCommitMode
    }
}


/**
 * Execute the [block] action within the scope of a transaction. Regardless of the
 * [autoCommit][Connection.getAutoCommit] state of the [Connection], autoCommit is turned off while
 * the [block] executes. If no errors where thrown, [Connection.commit] is called and the result of
 * the [block] is returned. If an exception is thrown, [Connection.rollback] is called and the
 * exception is rethrown. In all cases, the [autoCommit][Connection.getAutoCommit] state of the
 * [Connection] is returned back to the original state.
 *
 * @see withContext
 * @see Dispatchers.IO
 */
suspend inline fun <T> Connection.asTransactionSuspend(
    crossinline block: suspend Connection.() -> T,
): T = withContext(Dispatchers.IO) {
    val preCommitMode = this@asTransactionSuspend.autoCommit
    try {
        this@asTransactionSuspend.autoCommit = false
        val result = block(this@asTransactionSuspend)
        this@asTransactionSuspend.commit()
        result
    } catch (ex: Exception) {
        this@asTransactionSuspend.rollback()
        throw ex
    } finally {
        this@asTransactionSuspend.autoCommit = preCommitMode
    }
}
