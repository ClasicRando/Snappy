@file:Suppress("UNUSED")
package org.snappy.decode

import org.snappy.rowparse.SnappyRow
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime

class AnyDecoder : Decoder<Any> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Any? {
        return row.getAnyNullable(fieldName)
    }
}

class StringDecoder : Decoder<String> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): String? {
        return row.getStringNullable(fieldName)
    }
}

class BooleanDecoder : Decoder<Boolean> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Boolean? {
        return row.getBooleanNullable(fieldName)
    }
}

class ByteDecoder : Decoder<Byte> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Byte? {
        return row.getByteNullable(fieldName)
    }
}

class ShortDecoder : Decoder<Short> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Short? {
        return row.getShortNullable(fieldName)
    }
}

class IntDecoder : Decoder<Int> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Int? {
        return row.getIntNullable(fieldName)
    }
}

class LongDecoder : Decoder<Long> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Long? {
        return row.getLongNullable(fieldName)
    }
}

class FloatDecoder : Decoder<Float> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Float? {
        return row.getFloatNullable(fieldName)
    }
}

class DoubleDecoder : Decoder<Double> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Double? {
        return row.getDoubleNullable(fieldName)
    }
}

class BigDecimalDecoder : Decoder<BigDecimal> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): BigDecimal? {
        return row.getBigDecimalNullable(fieldName)
    }
}

class BytesDecoder : Decoder<ByteArray> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): ByteArray? {
        return row.getBytesNullable(fieldName)
    }
}

class DateDecoder : Decoder<Date> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Date? {
        return row.getDateNullable(fieldName)
    }
}

class TimeDecoder : Decoder<Time> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Time? {
        return row.getTimeNullable(fieldName)
    }
}

class TimestampDecoder : Decoder<Timestamp> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Timestamp? {
        return row.getTimestampNullable(fieldName)
    }
}

class InstantDecoder : Decoder<Instant> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): Instant? {
        return row.getInstantNullable(fieldName)
    }
}

class LocalDateDecoder : Decoder<LocalDate> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): LocalDate? {
        return row.getLocalDateNullable(fieldName)
    }
}

class LocalDateTimeDecoder : Decoder<LocalDateTime> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): LocalDateTime? {
        return row.getLocalDateTimeNullable(fieldName)
    }
}

class LocalTimeDecoder : Decoder<LocalTime> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): LocalTime? {
        return row.getLocalTimeNullable(fieldName)
    }
}

class OffsetDateTimeDecoder : Decoder<OffsetDateTime> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): OffsetDateTime? {
        return row.getOffsetDateTimeNullable(fieldName)
    }
}

class OffsetTimeDecoder : Decoder<OffsetTime> {
    override fun decodeNullable(row: SnappyRow, fieldName: String): OffsetTime? {
        return row.getOffsetTimeNullable(fieldName)
    }
}
