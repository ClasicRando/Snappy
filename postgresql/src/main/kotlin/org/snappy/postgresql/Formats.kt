package org.snappy.postgresql

import java.time.ZoneId
import java.time.format.DateTimeFormatter

/** Control characters found within postgresql literals. Need to be escaped/handled in encoding */
private val controlChars: Set<Char> = setOf('{', '}', '(', ')', '\\', '"')
/** [java.time.LocalTime] pattern for encoding date values into postgresql */
private const val LOCAL_TIME_PATTERN: String = "HH:mm:ss"
/** Formatter for the [java.time.LocalTime] pattern */
internal val localTimeFormatter = DateTimeFormatter.ofPattern(LOCAL_TIME_PATTERN)
/** [java.time.LocalDate] pattern for encoding date values into postgresql */
private const val LOCAL_DATE_PATTERN: String = "uuuu-MM-dd"
/** Formatter for the [java.time.LocalDate] pattern */
internal val localDateFormatter = DateTimeFormatter.ofPattern(LOCAL_DATE_PATTERN)
/** [java.time.Instant] pattern for encoding timestamp value into postgresql */
private const val INSTANT_DATE_PATTERN: String = "uuuu-MM-dd HH:mm:ss.SSSSSSx"
/** Formatter for [java.time.Instant] pattern */
internal val instantFormatter = DateTimeFormatter.ofPattern(INSTANT_DATE_PATTERN)
    .withZone(ZoneId.of("UTC"))
/**
 * [java.time.LocalDateTime] pattern for encoding timestamp without timezone value into postgresql
 */
private const val LOCAL_DATE_TIME_PATTERN: String = "uuuu-MM-dd HH:mm:ss.SSSSSS"
/** Formatter for [java.time.LocalDateTime] pattern */
internal val localDateTimeFormatter = DateTimeFormatter.ofPattern(LOCAL_DATE_TIME_PATTERN)
