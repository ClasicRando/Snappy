package org.snappy.data

import org.snappy.ksp.symbols.Rename
import org.snappy.ksp.symbols.RowParser

@RowParser
class AnnotatedTestClass {
    @Rename("simple_name")
    var complexFieldName: String = ""
    var otherFieldName: Long = 0
}
