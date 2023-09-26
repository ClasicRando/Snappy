package org.snappy.data

import org.snappy.decode.Decoder
import org.snappy.rowparse.SnappyRow

data class CompanionObjectDecoder(val field: String) {
    companion object : Decoder<CompanionObjectDecoder> {
        override fun decode(row: SnappyRow, fieldName: String): CompanionObjectDecoder? {
            return row.getStringNullable(fieldName)?.let { CompanionObjectDecoder(it)  }
        }
    }
}