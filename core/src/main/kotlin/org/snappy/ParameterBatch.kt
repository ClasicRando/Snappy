package org.snappy

import org.snappy.extensions.toSqlParameterList

interface ParameterBatch {
    fun toParameterBatch(): List<Any?>
}

fun ParameterBatch.toSqlParameterBatch(): List<SqlParameter> {
    return toParameterBatch().toSqlParameterList()
}
