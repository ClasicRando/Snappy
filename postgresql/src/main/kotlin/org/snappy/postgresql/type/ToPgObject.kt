package org.snappy.postgresql.type

import org.postgresql.util.PGobject
import org.snappy.encode.Encode
import java.sql.PreparedStatement

fun interface ToPgObject : Encode {
    fun toPgObject(): PGobject
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setObject(parameterIndex, toPgObject())
    }
}
