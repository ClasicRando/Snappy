package org.snappy.data

import org.snappy.SnappyColumn

data class AnnotatedTestDataClass(
    @SnappyColumn("simple_name")
    val complexFieldName: String,
    val otherFieldName: Long,
)
