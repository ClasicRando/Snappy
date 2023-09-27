package org.snappy.mssql.decode

import microsoft.sql.DateTimeOffset
import org.snappy.decode.Decoder
import org.snappy.rowparse.SnappyRow
import org.snappy.rowparse.getObjectNullable

/** Custom [Decoder] for [DateTimeOffset] */
class SqlServerDateTimeOffsetDecoder : Decoder<DateTimeOffset> {
    override fun decode(row: SnappyRow, fieldName: String): DateTimeOffset? {
        return row.getObjectNullable<DateTimeOffset>(fieldName)
    }
}
