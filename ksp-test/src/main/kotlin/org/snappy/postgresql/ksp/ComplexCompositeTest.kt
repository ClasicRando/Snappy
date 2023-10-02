package org.snappy.postgresql.ksp

import org.snappy.postgresql.type.PgType

@PgType("complex_composite_test")
data class ComplexCompositeTest(
    val intField: Int,
    val textField: String,
    val compositeField: SimpleCompositeTest,
    val intArrayField: List<Int?>,
    val compositeArrayField: List<SimpleCompositeTest?>,
)
