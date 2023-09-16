package org.snappy.data

import org.snappy.annotations.SnappyCacheRowParser
import org.snappy.annotations.SnappyColumn

@SnappyCacheRowParser
class AnnotatedTestClass {
    @SnappyColumn("simple_name")
    var complexFieldName: String = ""
    var otherFieldName: Long = 0
}
