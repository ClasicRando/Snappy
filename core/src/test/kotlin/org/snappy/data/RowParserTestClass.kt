package org.snappy.data

import org.snappy.rowparse.RowParser
import org.snappy.SnappyAutoCache
import org.snappy.SnappyRow

@SnappyAutoCache
class RowParserTestClass : RowParser<RowClass> {
    override fun parseRow(row: SnappyRow): RowClass {
        return RowClass("")
    }
}
