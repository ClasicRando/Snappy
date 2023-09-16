package org.snappy.postgresql.copy

fun interface IntoObjectRow {
    fun intoObjectRow(): Iterable<Any?>
}