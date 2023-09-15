package org.snappy.data

import org.snappy.annotations.SnappyColumn

data class AnnotatedTestDataClass(
    @SnappyColumn("simple_name")
    val complexFieldName: String,
    val otherFieldName: Long,
)
