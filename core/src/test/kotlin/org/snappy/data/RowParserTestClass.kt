package org.snappy.data

import org.snappy.rowparse.RowParser
import org.snappy.annotations.SnappyCacheRowParser
import org.snappy.rowparse.SnappyRow

@SnappyCacheRowParser
class RowParserTestClass : RowParser<RowClass> {
    override fun parseRow(row: SnappyRow): RowClass {
        return RowClass("")
    }
}
