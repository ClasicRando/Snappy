package org.snappy.mssql.tvp

/**
 * Denotes that a type can be used as a Table Valued Parameter row. This interface is used by
 * [AbstractTableType] to convert a row type to the underlining object array.
 */
fun interface ToTvpRow {
    /** Pack the properties of this row into an [Array] of objects */
    fun toTvpRow(): Array<Any?>
}
