package org.snappy.postgresql.uuid

import org.snappy.decode.Decoder
import org.snappy.rowparse.SnappyRow
import org.snappy.rowparse.getObjectNullable
import java.util.UUID

class UuidDecoder : Decoder<UUID> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): UUID? {
        return row.getObjectNullable<UUID>(fieldName)
    }
}
