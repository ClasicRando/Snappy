package org.snappy.postgresql.type

import org.postgresql.util.PGobject
import org.snappy.decode.Decoder
import org.snappy.rowparse.SnappyRow
import org.snappy.rowparse.getObjectNullable

interface PgObjectDecoder<T : Any> : Decoder<T> {
    fun decodePgObjectValue(value: String): T? {
        throw NotImplementedError("Default PG object value decoder called")
    }

    fun decodePgObject(pgObject: PGobject): T? {
        return pgObject.value?.let {
            decodePgObjectValue(it)
        }
    }

    override fun decode(row: SnappyRow, fieldName: String): T? {
        return row.getObjectNullable<PGobject>(fieldName)?.let {
            decodePgObject(it)
        }
    }
}
