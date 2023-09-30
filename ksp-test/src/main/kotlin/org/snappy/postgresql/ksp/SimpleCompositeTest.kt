package org.snappy.postgresql.ksp

import org.postgresql.util.PGobject
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
    val boolField: Boolean?,
    val smallintField: Short?,
    val intField: Int?,
    val bigintField: Long?,
    val realField: Float?,
    val doubleField: Double?,
    val textField: String?,
    val numericField: BigDecimal?,
    val dateField: LocalDate?,
    val timestampField: LocalDateTime?,
    val timestampTzField: OffsetDateTime?,
    val timeField: LocalTime?,
    val timeTzField: OffsetTime?,
) : ToPgObject {
    override fun toPgObject(): PGobject {
        TODO("Not yet implemented")
    }
}
