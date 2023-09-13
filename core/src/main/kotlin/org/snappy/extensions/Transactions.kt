package org.snappy.extensions

import java.lang.Exception
import java.sql.Connection

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
