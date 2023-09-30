package org.snappy.data

import org.snappy.annotations.Rename
import org.snappy.ksp.RowParser

@RowParser
data class AnnotatedTestDataClass(
    @Rename("simple_name")
    val complexFieldName: String,
    val otherFieldName: Long,
)
