package org.snappy.postgresql.copy

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigDecimal
import java.time.Instant
import java.time.ZoneOffset
import java.util.stream.Stream
import kotlin.test.assertEquals

class FormatObjectTest {
    @ParameterizedTest
    @MethodSource("valueToExpectedPairs")
    fun `formatObject should return expected value`(args: Pair<Any?, String>) {
        val (value, expected) = args

        val result = formatObject(value)

        assertEquals(expected, result)
    }

    companion object {
        @JvmStatic
        fun valueToExpectedPairs(): Stream<Pair<Any?, String>> {
            val januaryFirstText = "2023-01-01T00:00:00Z"
            val janFirstInstant = Instant.parse(januaryFirstText)
            val estOffsetDateTime = janFirstInstant.atOffset(ZoneOffset.ofHours(-5))
            val localDateTime = janFirstInstant.atOffset(ZoneOffset.UTC).toLocalDateTime()
            return Stream.of(
                null to "",
                "Test" to "Test",
                BigDecimal("52.524688579") to "52.524688579",
                "This is a test".toByteArray() to "This is a test",
                janFirstInstant to januaryFirstText,
                estOffsetDateTime to "2022-12-31T19:00:00-05:00",
                estOffsetDateTime.toOffsetTime() to "19:00:00-05:00",
                localDateTime to januaryFirstText.trim('Z'),
                localDateTime.toLocalDate() to "2023-01-01",
                localDateTime.toLocalTime() to "00:00:00",
                1 to "1",
                22.53652 to "22.53652",
                54L to "54",
                0.52654F to "0.52654",
            )
        }
    }
}
