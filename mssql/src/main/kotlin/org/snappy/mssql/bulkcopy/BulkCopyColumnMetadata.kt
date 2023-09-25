package org.snappy.mssql.bulkcopy

data class BulkCopyColumnMetadata(
    val name: String,
    val type: Int,
    val precision: Int?,
    val scale: Int?,
)
