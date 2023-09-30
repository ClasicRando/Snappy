package org.snappy.data

import org.snappy.ksp.RowParser

@RowParser
data class SimpleTestDataClass(
    val stringField: String,
    val booleanField: Boolean,
    val shortField: Short,
    val intField: Int,
    val longField: Long,
    val doubleField: Double,
    val floatField: Float,
    val nullField: Any?,
)
