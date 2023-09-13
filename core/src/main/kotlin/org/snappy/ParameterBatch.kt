package org.snappy

import org.snappy.extensions.toSqlParameterList

fun interface ParameterBatch {
    fun toParameterBatch(): List<Any?>
}

internal fun ParameterBatch.toSqlParameterBatch(): List<SqlParameter> {
    return toParameterBatch().toSqlParameterList()
}
