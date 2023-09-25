package org.snappy.postgresql.data

import org.snappy.postgresql.copy.ToCsvRow
import org.snappy.copy.ToObjectRow
import org.snappy.postgresql.copy.formatObject
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import kotlin.random.Random

data class CsvRow(
    val boolField: Boolean,
    val smallintField: Short,
    val intField: Int,
    val bigintField: Long,
    val realField: Float,
    val doubleField: Double,
    val textField: String,
    val numericField: BigDecimal,
    val dateField: LocalDate,
    val timestampField: LocalDateTime,
    val timestampTzField: OffsetDateTime,
    val timeField: LocalTime,
    val timeTzField: OffsetTime
) : ToObjectRow, ToCsvRow {

    override fun toObjectRow(): Iterable<Any?> {
        return listOf(
            boolField,
            smallintField,
            intField,
            bigintField,
            realField,
            doubleField,
            textField,
            numericField,
            dateField,
            timestampField,
            timestampTzField,
            timeField,
            timeTzField,
        )
    }

    override fun toCsvRow(): Iterable<String> {
        return toObjectRow().map { formatObject(it) }
    }

    companion object {
        fun random(): CsvRow {
            val random = Random(System.currentTimeMillis())
            return CsvRow(
                random.nextBoolean(),
                random.nextInt(Short.MAX_VALUE.toInt()).toShort(),
                random.nextInt(),
                random.nextLong(),
                random.nextFloat(),
                random.nextDouble(),
                "",
                BigDecimal.valueOf(random.nextDouble()),
                LocalDate.ofEpochDay(random.nextLong(0, 30000)),
                LocalDateTime.ofEpochSecond(random.nextLong(0, 2524554080), 0, ZoneOffset.UTC),
                LocalDateTime.ofEpochSecond(random.nextLong(0, 2524554080), 0, ZoneOffset.UTC)
                    .atOffset(ZoneOffset.ofHours(random.nextInt(-12, 12))),
                LocalTime.ofSecondOfDay(random.nextLong(0, 86400)),
                LocalTime.ofSecondOfDay(random.nextLong(0, 86400))
                    .atOffset(ZoneOffset.ofHours(random.nextInt(-12, 12)))
            )
        }
    }
}
