package org.snappy.postgresql.data

import org.postgresql.util.PGobject
import org.snappy.postgresql.literal.PgCompositeLiteralBuilder
import org.snappy.postgresql.literal.parseComposite
import org.snappy.postgresql.type.PgObjectDecoder
import org.snappy.postgresql.type.PgType
import org.snappy.postgresql.type.ToPgObject
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime

@PgType("simple_composite_test")
data class SimpleCompositeTest(
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
    val timeTzField: OffsetTime,
) : ToPgObject {
    override fun toPgObject(): PGobject {
        val literal = PgCompositeLiteralBuilder()
            .appendBoolean(boolField)
            .appendShort(smallintField)
            .appendInt(intField)
            .appendLong(bigintField)
            .appendFloat(realField)
            .appendDouble(doubleField)
            .appendString(textField)
            .appendBigDecimal(numericField)
            .appendLocalDate(dateField)
            .appendLocalDateTime(timestampField)
            .appendOffsetDateTime(timestampTzField)
            .appendLocalTime(timeField)
            .appendOffsetTime(timeTzField)
            .toString()
        return PGobject().apply {
            type = "simple_composite_test"
            value = literal
        }
    }

    companion object : PgObjectDecoder<SimpleCompositeTest> {
        val default = SimpleCompositeTest(
            true,
            0,
            0,
            0,
            0F,
            0.0,
            "",
            BigDecimal.ONE,
            LocalDate.now(),
            LocalDateTime.now(),
            OffsetDateTime.now(),
            LocalTime.now(),
            OffsetTime.now(),
        )

        override fun decodePgObject(pgObject: PGobject): SimpleCompositeTest? {
            return parseComposite(pgObject) {
                val boolField = readBoolean() ?: error("bool field cannot be null")
                val smallintField = readShort() ?: error("short field cannot be null")
                val intField = readInt() ?: error("int field cannot be null")
                val bigintField = readLong() ?: error("long field cannot be null")
                val realField = readFloat() ?: error("float field cannot be null")
                val doubleField = readDouble() ?: error("double field cannot be null")
                val textField = readString() ?: error("string field cannot be null")
                val numericField = readBigDecimal() ?: error("numeric field cannot be null")
                val dateField = readLocalDate() ?: error("local date field cannot be null")
                val timestampField = readLocalDateTime()
                    ?: error("local date time field cannot be null")
                val timestampTzField = readOffsetDateTime()
                    ?: error("offset date time field cannot be null")
                val timeField = readLocalTime() ?: error("local time field cannot be null")
                val timeTzField = readOffsetTime() ?: error("offset time field cannot be null")
                SimpleCompositeTest(
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
        }
    }
}
