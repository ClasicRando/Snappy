package org.snappy.decode

import org.snappy.rowparse.SnappyRow
import org.snappy.rowparse.getObjectNullable
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime

class InstantDecoder : Decoder<Instant> {
    override fun decode(row: SnappyRow, fieldName: String): Instant? {
        return row.getObjectNullable<Instant>(fieldName)
    }
}

class LocalDateDecoder : Decoder<LocalDate> {
    override fun decode(row: SnappyRow, fieldName: String): LocalDate? {
        return row.getObjectNullable<LocalDate>(fieldName)
    }
}

class LocalDateTimeDecoder : Decoder<LocalDateTime> {
    override fun decode(row: SnappyRow, fieldName: String): LocalDateTime? {
        return row.getObjectNullable<LocalDateTime>(fieldName)
    }
}

class LocalTimeDecoder : Decoder<LocalTime> {
    override fun decode(row: SnappyRow, fieldName: String): LocalTime? {
        return row.getObjectNullable<LocalTime>(fieldName)
    }
}

class OffsetDateTimeDecoder : Decoder<OffsetDateTime> {
    override fun decode(row: SnappyRow, fieldName: String): OffsetDateTime? {
        return row.getObjectNullable<OffsetDateTime>(fieldName)
    }
}

class OffsetTimeDecoder : Decoder<OffsetTime> {
    override fun decode(row: SnappyRow, fieldName: String): OffsetTime? {
        return row.getObjectNullable<OffsetTime>(fieldName)
    }
}
