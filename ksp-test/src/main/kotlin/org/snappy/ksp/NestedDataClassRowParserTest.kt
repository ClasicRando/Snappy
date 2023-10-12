package org.snappy.ksp

import org.snappy.ksp.symbols.Flatten
import org.snappy.ksp.symbols.Rename
import org.snappy.ksp.symbols.RowParser

@RowParser
data class NestedDataClassRowParserTest(
    @Rename("name")
    val test: String,
    @Flatten
    val dataClass: DataClassRowParserTest
)
