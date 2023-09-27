package org.snappy.mssql.bulkcopy

/**
 * Container for metadata of bulk copy columns, required for [SequenceBulkCopy] and csv copy methods
 */
internal data class BulkCopyColumnMetadata(
    val name: String,
    val type: Int,
    val precision: Int?,
    val scale: Int?,
)
