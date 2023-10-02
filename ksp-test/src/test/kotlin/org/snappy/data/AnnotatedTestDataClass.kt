package org.snappy.data

import org.snappy.ksp.symbols.Rename
import org.snappy.ksp.symbols.RowParser

@RowParser
data class AnnotatedTestDataClass(
    @Rename("simple_name")
    val complexFieldName: String,
    val otherFieldName: Long,
)
