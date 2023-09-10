package org.snappy.data

import org.snappy.SnappyAutoCache
import org.snappy.SnappyColumn

@SnappyAutoCache
class AnnotatedTestClass {
    @SnappyColumn("simple_name")
    var complexFieldName: String = ""
    var otherFieldName: Long = 0
}
