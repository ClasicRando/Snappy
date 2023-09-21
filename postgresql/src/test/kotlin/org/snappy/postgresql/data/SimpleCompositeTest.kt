package org.snappy.postgresql.data

import org.postgresql.util.PGobject
import org.snappy.postgresql.type.PgCompositeLiteralBuilder
import org.snappy.postgresql.type.PgObjectDecoder
import org.snappy.postgresql.type.PgType
import org.snappy.postgresql.type.ToPgObject
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import kotlin.reflect.KClass

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

        override val decodeClass: KClass<SimpleCompositeTest> = SimpleCompositeTest::class

        override fun decodePgObject(pgObject: PGobject): SimpleCompositeTest {
            TODO("Not yet implemented")
        }
    }
}
