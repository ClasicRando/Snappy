package org.snappy.postgresql.data

import org.junit.jupiter.api.Assertions
import org.postgresql.util.PGobject
import org.snappy.postgresql.array.toList
import org.snappy.rowparse.RowParser
import org.snappy.rowparse.SnappyRow
import java.sql.Array
import kotlin.test.assertEquals

data class ComplexCompositeTestResult(
    val textField1: String,
    val textField2: String,
    val intField1: Int,
    val intField2: Int,
    val compositeField1: PGobject,
    val compositeField2: PGobject,
    val intArrayField1: List<Int>,
    val intArrayField2: List<Int>,
    val compositeArrayField1: List<PGobject>,
    val compositeArrayField2: List<PGobject>,
) {
    fun checkEquality() {
        assertEquals(textField1, textField2)
        assertEquals(intField1, intField2)
        assertEquals(compositeField1.value, compositeField2.value)
        Assertions.assertIterableEquals(intArrayField1, intArrayField2)
        Assertions.assertIterableEquals(compositeArrayField1, compositeArrayField2)
    }

    companion object : RowParser<ComplexCompositeTestResult> {
        override fun parseRow(row: SnappyRow): ComplexCompositeTestResult {
            return ComplexCompositeTestResult(
                textField1 = row.getAsNotNull("text_field_1"),
                textField2 = row.getAsNotNull("text_field_2"),
                intField1 = row.getAsNotNull("int_field_1"),
                intField2 = row.getAsNotNull("int_field_2"),
                compositeField1 = row.getAsNotNull("composite_field_1"),
                compositeField2 = row.getAsNotNull("composite_field_2"),
                intArrayField1 = row.getAsNotNull<Array>("int_array_field_1").toList(),
                intArrayField2 = row.getAsNotNull<Array>("int_array_field_2").toList(),
                compositeArrayField1 = row.getAsNotNull<Array>("composite_array_field_1").toList(),
                compositeArrayField2 = row.getAsNotNull<Array>("composite_array_field_2").toList(),
            )
        }
    }
}