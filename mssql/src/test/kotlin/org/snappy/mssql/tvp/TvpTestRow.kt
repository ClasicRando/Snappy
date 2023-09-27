package org.snappy.mssql.tvp

import org.snappy.annotations.SnappyColumn
import org.snappy.copy.ToObjectRow
import org.snappy.mssql.DateTime
import org.snappy.mssql.SmallDateTime
import org.snappy.mssql.toDateTime
import org.snappy.mssql.toSmallDateTime
import java.math.BigDecimal
import java.math.RoundingMode
import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.random.Random

data class TvpTestRow(
    @SnappyColumn("bool_field")
    val boolField: Boolean,
    @SnappyColumn("smallint_field")
    val smallintField: Short,
    @SnappyColumn("int_field")
    val intField: Int,
    @SnappyColumn("bigint_field")
    val bigintField: Long,
    @SnappyColumn("real_field")
    val realField: Float,
    @SnappyColumn("double_field")
    val doubleField: Double,
    @SnappyColumn("text_field")
    val textField: String,
    @SnappyColumn("numeric_field")
    val numericField: BigDecimal,
    @SnappyColumn("date_field")
    val dateField: LocalDate,
    @SnappyColumn("datetime_field")
    val datetimeField: DateTime,
    @SnappyColumn("smalldatetime_field")
    val smallDateTimeField: SmallDateTime,
    @SnappyColumn("datetimeoffset_field")
    val dateTimeOffsetField: microsoft.sql.DateTimeOffset,
    @SnappyColumn("time_field")
    val timeField: LocalTime,
) : ToTvpRow, ToObjectRow {
    override fun toTvpRow(): Array<Any?> {
        return arrayOf(
            boolField,
            smallintField,
            intField,
            bigintField,
            realField,
            doubleField,
            textField,
            numericField,
            dateField,
            datetimeField.toString(),
            smallDateTimeField.toString(),
            dateTimeOffsetField,
            timeField
        )
    }

    override fun toObjectRow(): List<Any?> {
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
            datetimeField,
            smallDateTimeField.bulkCopyString(),
            dateTimeOffsetField,
            timeField
        )
    }

    companion object {
        fun random(): TvpTestRow {
            val random = Random(System.currentTimeMillis())
            val timestamp = LocalDateTime.ofEpochSecond(
                random.nextLong(0, 2524554080),
                0,
                ZoneOffset.UTC
            ).atOffset(ZoneOffset.ofHours(random.nextInt(-12, 12)))
            return TvpTestRow(
                random.nextBoolean(),
                random.nextInt(Short.MAX_VALUE.toInt()).toShort(),
                random.nextInt(),
                random.nextLong(),
                random.nextFloat(),
                random.nextDouble(),
                UUID.randomUUID().toString(),
                BigDecimal(random.nextDouble() * 10_000).setScale(5, RoundingMode.FLOOR),
                LocalDate.ofEpochDay(random.nextLong(0, 30000)),
                LocalDateTime.ofEpochSecond(random.nextLong(0, 2524554080), 0, ZoneOffset.UTC)
                    .toDateTime(),
                LocalDateTime.ofEpochSecond(random.nextLong(0, 2524554080), 0, ZoneOffset.UTC)
                    .toSmallDateTime(),
                microsoft.sql.DateTimeOffset.valueOf(
                    Timestamp.valueOf(timestamp.toLocalDateTime()),
                    timestamp.offset.totalSeconds / 60,
                ),
                LocalTime.ofSecondOfDay(random.nextLong(0, 86400)),
            )
        }
    }
}
