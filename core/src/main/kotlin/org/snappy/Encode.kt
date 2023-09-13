package org.snappy

import java.sql.PreparedStatement

fun interface Encode {
    fun encode(preparedStatement: PreparedStatement, parameterIndex: Int)
}
