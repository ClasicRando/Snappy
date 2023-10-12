package org.snappy.postgresql.json

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement
import org.postgresql.util.PGobject
import org.snappy.decode.Decoder
import org.snappy.postgresql.type.ToPgObject
import org.snappy.rowparse.SnappyRow

@Serializable
class PgJson internal constructor(@PublishedApi internal val json: JsonElement) : ToPgObject {

    constructor(json: String): this(Json.parseToJsonElement(json))

    inline fun <reified T> decode(): T {
        return Json.decodeFromJsonElement(json)
    }

    override fun toPgObject(): PGobject {
        return PGobject().apply {
            type = "jsonb"
            value = Json.encodeToString(json)
        }
    }

    companion object : Decoder<PgJson> {
        override fun decodeNullable(row: SnappyRow, fieldName: String): PgJson? {
            return row.getStringNullable(fieldName)?.let {
                PgJson(Json.decodeFromString<JsonElement>(it))
            }
        }
    }
}
