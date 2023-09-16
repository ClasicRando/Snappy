package org.snappy.postgresql.copy

fun interface IntoCsvRow {
    fun intoCsvRow(): Iterable<String>
}