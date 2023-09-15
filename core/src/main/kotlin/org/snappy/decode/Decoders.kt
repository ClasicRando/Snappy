package org.snappy.decode

import org.snappy.annotations.SnappyCacheDecoder
import org.snappy.decodeError
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

@SnappyCacheDecoder
class InstantDecoder : Decoder<Instant> {
    override fun decode(value: Any): Instant {
        return when (value) {
            is Instant -> value
            is Timestamp -> Instant.ofEpochMilli(value.time)
            is String -> {
                val dateTime = runCatching {
                    LocalDateTime.parse(value)
                }.getOrNull() ?: runCatching {
                    LocalDateTime.parse(
                        value,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]"),
                    )
                }.getOrNull() ?: decodeError(Instant::class, value)
                dateTime.toInstant(ZoneOffset.UTC)
            }
            else -> decodeError(Instant::class, value)
        }
    }
}
