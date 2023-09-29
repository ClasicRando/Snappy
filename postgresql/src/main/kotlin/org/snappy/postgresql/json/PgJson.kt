package org.snappy.postgresql.json

import kotlinx.serialization.json.Json
import org.postgresql.util.PGobject
import org.snappy.decode.Decoder
import org.snappy.postgresql.type.ToPgObject
import org.snappy.rowparse.SnappyRow

class PgJson internal constructor(@PublishedApi internal val json: ByteArray) : ToPgObject {

    constructor(json: String): this(json.toByteArray())

    inline fun <reified T> decode(): T {
        return Json.decodeFromString<T>(json.decodeToString())
    }

    override fun toPgObject(): PGobject {
        return PGobject().apply {
            type = "jsonb"
            value = json.decodeToString()
        }
    }

    companion object : Decoder<PgJson> {
        override fun decode(row: SnappyRow, fieldName: String): PgJson? {
            return row.getBytesNullable(fieldName)?.let {
                PgJson(it)
            }
        }
    }
}
