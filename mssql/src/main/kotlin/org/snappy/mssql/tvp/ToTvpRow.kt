package org.snappy.mssql.tvp

fun interface ToTvpRow {
    fun toTvpRow(): Array<Any?>
}
