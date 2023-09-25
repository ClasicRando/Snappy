package org.snappy.mssql.bulkcopy

internal data class BulkCopyColumnMetadata(
    val name: String,
    val type: Int,
    val precision: Int?,
    val scale: Int?,
)
