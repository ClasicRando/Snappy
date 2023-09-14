package org.snappy.data

import org.snappy.rowparse.RowParser
import org.snappy.SnappyAutoCache
import org.snappy.SnappyRow

@SnappyAutoCache
data class CompanionObjectParser(val field: String) {
    companion object : RowParser<CompanionObjectParser> {
        override fun parseRow(row: SnappyRow): CompanionObjectParser {
            return CompanionObjectParser(row.getAs("field")!!)
        }
    }
}
