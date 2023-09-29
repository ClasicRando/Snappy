package org.snappy.postgresql.type

import org.postgresql.util.PGobject
import org.snappy.encode.Encode
import java.sql.PreparedStatement

/**
 * Classes that implement this interface have the ability to convert it's content to a [PGobject]
 * which can then be encoded to a [PreparedStatement] since it implements [Encode] with a default
 * implementation of [Encode.encode].
 */
fun interface ToPgObject : Encode {
    /** Convert this instance into a [PGobject] */
    fun toPgObject(): PGobject
    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setObject(parameterIndex, toPgObject())
    }
}
