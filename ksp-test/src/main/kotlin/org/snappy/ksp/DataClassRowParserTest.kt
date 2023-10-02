package org.snappy.ksp

import org.snappy.ksp.symbols.Rename
import org.snappy.ksp.symbols.RowParser

@RowParser
data class DataClassRowParserTest(@Rename("name") val test: String)
