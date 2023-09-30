package org.snappy.data

import org.snappy.annotations.Rename
import org.snappy.ksp.RowParser

@RowParser
class AnnotatedTestClass {
    @Rename("simple_name")
    var complexFieldName: String = ""
    var otherFieldName: Long = 0
}
