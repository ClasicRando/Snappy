package org.snappy.data

import org.snappy.rowparse.RowParser
import org.snappy.annotations.SnappyCacheRowParser
import org.snappy.rowparse.SnappyRow

@SnappyCacheRowParser
data class CompanionObjectParser(val field: String) {
    companion object : RowParser<CompanionObjectParser> {
        override fun parseRow(row: SnappyRow): CompanionObjectParser {
            return CompanionObjectParser(row.getString("field"))
        }
    }
}
