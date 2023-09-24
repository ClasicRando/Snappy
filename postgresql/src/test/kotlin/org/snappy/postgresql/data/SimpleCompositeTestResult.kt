package org.snappy.postgresql.data

import org.snappy.rowparse.RowParser
import org.snappy.rowparse.SnappyRow
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import kotlin.test.assertEquals

data class SimpleCompositeTestResult(
    val boolField1: Boolean,
    val boolField2: Boolean,
    val smallIntField1: Short,
    val smallIntField2: Short,
    val intField1: Int,
    val intField2: Int,
    val bigIntField1: Long,
    val bigIntField2: Long,
    val realField1: Float,
    val realField2: Float,
    val doubleField1: Double,
    val doubleField2: Double,
    val textField1: String,
    val textField2: String,
    val numericField1: BigDecimal,
    val numericField2: BigDecimal,
    val dateField1: Date,
    val dateField2: Date,
    val timestampField1: Timestamp,
    val timestampField2: Timestamp,
    val timestampTzField1: Timestamp,
    val timestampTzField2: Timestamp,
    val timeField1: Time,
    val timeField2: Time,
    val timeTzField1: Time,
    val timeTzField2: Time,
) {
    fun checkEquality() {
        assertEquals(boolField1, boolField2)
        assertEquals(smallIntField1, smallIntField2)
        assertEquals(intField1, intField2)
        assertEquals(bigIntField1, bigIntField2)
        assertEquals(realField1, realField2)
        assertEquals(doubleField1, doubleField2)
        assertEquals(textField1, textField2)
        assertEquals(numericField1, numericField2)
        assertEquals(dateField1, dateField2)
        assertEquals(timestampField2, timestampField2)
        assertEquals(timestampTzField1, timestampTzField2)
        assertEquals(timeField1, timeField2)
        assertEquals(timeTzField1, timeTzField2)
    }

    companion object : RowParser<SimpleCompositeTestResult> {
        override fun parseRow(row: SnappyRow): SimpleCompositeTestResult {
            return SimpleCompositeTestResult(
                boolField1 = row.getBoolean("bool_field1"),
                boolField2 = row.getBoolean("bool_field2"),
                smallIntField1 = row.getInt("smallint_field1").toShort(),
                smallIntField2 = row.getInt("smallint_field2").toShort(),
                intField1 = row.getInt("int_field1"),
                intField2 = row.getInt("int_field2"),
                bigIntField1 = row.getLong("bigint_field1"),
                bigIntField2 = row.getLong("bigint_field2"),
                realField1 = row.getFloat("real_field1"),
                realField2 = row.getFloat("real_field2"),
                doubleField1 = row.getDouble("double_field1"),
                doubleField2 = row.getDouble("double_field2"),
                textField1 = row.getString("text_field1"),
                textField2 = row.getString("text_field2"),
                numericField1 = row.getBigDecimal("numeric_field1"),
                numericField2 = row.getBigDecimal("numeric_field2"),
                dateField1 = row.getDate("date_field1"),
                dateField2 = row.getDate("date_field2"),
                timestampField1 = row.getTimestamp("timestamp_field1"),
                timestampField2 = row.getTimestamp("timestamp_field2"),
                timestampTzField1 = row.getTimestamp("timestamptz_field1"),
                timestampTzField2 = row.getTimestamp("timestamptz_field2"),
                timeField1 = row.getTime("time_field1"),
                timeField2 = row.getTime("time_field2"),
                timeTzField1 = row.getTime("timetz_field1"),
                timeTzField2 = row.getTime("timetz_field2"),
            )
        }
    }
}