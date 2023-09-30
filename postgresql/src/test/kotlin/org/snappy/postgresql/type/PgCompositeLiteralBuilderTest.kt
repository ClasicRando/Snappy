package org.snappy.postgresql.type

import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.util.PGobject
import org.snappy.postgresql.data.ComplexCompositeTestResult
import org.snappy.postgresql.data.SimpleCompositeTestResult
import org.snappy.postgresql.literal.PgCompositeLiteralBuilder
import org.snappy.query.queryFirst
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.BeforeTest
import kotlin.test.Test

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class PgCompositeLiteralBuilderTest {
    private val missingEnvironmentVariableMessage = "To run Postgres tests the environment " +
            "variable SNAPPY_PG_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (Connection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_PG_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString).use(action)
    }

    private fun readResource(name: String): String {
        return this::class.java
            .classLoader
            .getResource(name)
            ?.openStream()?.use { stream ->
                stream.bufferedReader().readText()
            } ?: throw IllegalStateException("Could not find '$name'")
    }

    @BeforeTest
    fun setup() {
        val startupScript = readResource("start_pg_composite_test.pgsql")
        useConnection { c ->
            c.createStatement().use { s ->
                s.execute(startupScript)
            }
        }
    }

    @Test
    fun `simple composite built matches values within database`() {
        val compositeCompareQuery = readResource("simple_composite_compare_query.pgsql")
        useConnection { c ->
            val janFirst = LocalDateTime.of(2023, 1, 1, 0, 0, 0)
            val builder = PgCompositeLiteralBuilder()
                .appendBoolean(true)
                .appendShort(1)
                .appendInt(1)
                .appendLong(1)
                .appendFloat(1F)
                .appendDouble(1.0)
                .appendString("Test")
                .appendBigDecimal(BigDecimal("1"))
                .appendLocalDate(janFirst.toLocalDate())
                .appendLocalDateTime(janFirst)
                .appendInstant(janFirst.toInstant(ZoneOffset.UTC))
                .appendLocalTime(janFirst.toLocalTime())
                .appendLocalTime(janFirst.toLocalTime())
            val compositeLiteral = builder.toString()
            val composite = PGobject().apply {
                type = "simple_composite_test"
                value = compositeLiteral
            }

            val result: SimpleCompositeTestResult = assertDoesNotThrow {
                c.queryFirst<SimpleCompositeTestResult>(compositeCompareQuery, listOf(composite))
            }
            result.checkEquality()
        }
    }

    @Test
    fun `complex composite built matches values within database`() {
        val compositeCompareQuery = readResource("complex_composite_compare_query.pgsql")
        useConnection { c ->
            val janFirst = LocalDateTime.of(2023, 1, 1, 0, 0, 0)
            val builder = PgCompositeLiteralBuilder()
                .appendBoolean(true)
                .appendShort(1)
                .appendInt(1)
                .appendLong(1)
                .appendFloat(1F)
                .appendDouble(1.0)
                .appendString("Test")
                .appendBigDecimal(BigDecimal("1"))
                .appendLocalDate(janFirst.toLocalDate())
                .appendLocalDateTime(janFirst)
                .appendInstant(janFirst.toInstant(ZoneOffset.UTC))
                .appendLocalTime(janFirst.toLocalTime())
                .appendLocalTime(janFirst.toLocalTime())
            val simpleComposite = PGobject().apply {
                type = "complex_composite_test"
                value = builder.toString()
            }

            val builder2 = PgCompositeLiteralBuilder()
                .appendString("This is a test")
                .appendInt(2314)
                .appendComposite(ToPgObject { simpleComposite })
                .appendArray(arrayOf(1,5,6,4,5))
                .appendArray(arrayOf(ToPgObject { simpleComposite }))
            val compositeLiteral = builder2.toString()
            val composite = PGobject().apply {
                type = "complex_composite_test"
                value = compositeLiteral
            }

            val result: ComplexCompositeTestResult = assertDoesNotThrow {
                c.queryFirst<ComplexCompositeTestResult>(compositeCompareQuery, listOf(composite))
            }
            result.checkEquality()
        }
    }
}
