package org.snappy.ksp

import org.snappy.annotations.Rename

@RowParser
data class DataClassRowParserTest(@Rename("name") val test: String)
