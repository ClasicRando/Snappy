package org.snappy.decode

import org.snappy.annotations.SnappyCacheDecoder
import org.snappy.decodeError
import java.math.BigDecimal
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/** Wrapper for [Boolean] to allow for encoding to a [PreparedStatement] */
internal class AnyDecoder : Decoder<Any> {
    override fun decode(value: Any): Any = value as? Any ?: decodeError<Any>(value)
}

/** Wrapper for [Boolean] to allow for encoding to a [PreparedStatement] */
internal class BooleanDecoder : Decoder<Boolean> {
    override fun decode(value: Any): Boolean = value as? Boolean ?: decodeError<Boolean>(value)
}

/** Wrapper for [Byte] to allow for encoding to a [PreparedStatement] */
internal class ByteDecoder : Decoder<Byte> {
    override fun decode(value: Any): Byte = value as? Byte ?: decodeError<Byte>(value)
}

/** Wrapper for [Short] to allow for encoding to a [PreparedStatement] */
internal class ShortDecoder : Decoder<Short> {
    override fun decode(value: Any): Short = value as? Short ?: decodeError<Short>(value)
}

/** Wrapper for [Int] to allow for encoding to a [PreparedStatement] */
internal class IntDecoder: Decoder<Int> {
    override fun decode(value: Any): Int = value as? Int ?: decodeError<Int>(value)
}

/** Wrapper for [Long] to allow for encoding to a [PreparedStatement] */
internal class LongDecoder: Decoder<Long> {
    override fun decode(value: Any): Long = value as? Long ?: decodeError<Long>(value)
}

/** Wrapper for [Float] to allow for encoding to a [PreparedStatement] */
internal class FloatDecoder: Decoder<Float> {
    override fun decode(value: Any): Float = value as? Float ?: decodeError<Float>(value)
}

/** Wrapper for [Double] to allow for encoding to a [PreparedStatement] */
internal class DoubleDecoder: Decoder<Double> {
    override fun decode(value: Any): Double = value as? Double ?: decodeError<Double>(value)
}

/** Wrapper for [BigDecimal] to allow for encoding to a [PreparedStatement] */
internal class BigDecimalDecoder: Decoder<BigDecimal> {
    override fun decode(value: Any): BigDecimal = value as? BigDecimal ?: decodeError<BigDecimal>(value)
}

/** Wrapper for [Date] to allow for encoding to a [PreparedStatement] */
internal class DateDecoder: Decoder<Date> {
    override fun decode(value: Any): Date = value as? Date ?: decodeError<Date>(value)
}

/** Wrapper for [Timestamp] to allow for encoding to a [PreparedStatement] */
internal class TimestampDecoder: Decoder<Timestamp> {
    override fun decode(value: Any): Timestamp = value as? Timestamp ?: decodeError<Timestamp>(value)
}

/** Wrapper for [Time] to allow for encoding to a [PreparedStatement] */
internal class TimeDecoder: Decoder<Time> {
    override fun decode(value: Any): Time = value as? Time ?: decodeError<Time>(value)
}

/** Wrapper for [String] to allow for encoding to a [PreparedStatement] */
internal class StringDecoder: Decoder<String> {
    override fun decode(value: Any): String = value as? String ?: decodeError<String>(value)
}

/** Wrapper for [ByteArray] to allow for encoding to a [PreparedStatement] */
internal class ByteArrayDecoder: Decoder<ByteArray> {
    override fun decode(value: Any): ByteArray = value as? ByteArray ?: decodeError<ByteArray>(value)
}

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
