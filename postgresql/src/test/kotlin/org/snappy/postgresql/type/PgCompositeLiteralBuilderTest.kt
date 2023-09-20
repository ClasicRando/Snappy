package org.snappy.postgresql.type

import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.util.PGobject
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

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

    private fun checkEquality(values: Array<Pair<Any?, Any?>>) {
        for ((expected, actual) in values) {
            when (expected) {
                is PGobject -> assertEquals(expected.value, (actual as PGobject).value)
                is java.sql.Array -> {
                    val arr1 = expected.array as Array<*>
                    val arr2 = (actual as java.sql.Array).array as Array<*>
                    checkEquality(arr1.zip(arr2).toTypedArray())
                }
                else -> assertEquals(expected, actual)
            }

        }
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

            val result: Array<Pair<Any?, Any?>> = assertDoesNotThrow {
                c.prepareStatement(compositeCompareQuery).use { s ->
                    s.setObject(
                        1,
                        PGobject().apply {
                            type = "simple_composite_test"
                            value = compositeLiteral
                        },
                    )
                    s.executeQuery().use { rs ->
                        rs.next()
                        arrayOf(
                            rs.getBoolean(1) to rs.getBoolean(14),
                            rs.getShort(2)to rs.getShort(15),
                            rs.getInt(3)to rs.getInt(16),
                            rs.getLong(4)to rs.getLong(17),
                            rs.getFloat(5)to rs.getFloat(18),
                            rs.getDouble(6)to rs.getDouble(19),
                            rs.getString(7)to rs.getString(20),
                            rs.getBigDecimal(8)to rs.getBigDecimal(21),
                            rs.getDate(9)to rs.getDate(22),
                            rs.getTimestamp(10)to rs.getTimestamp(23),
                            rs.getTimestamp(11)to rs.getTimestamp(24),
                            rs.getTime(12)to rs.getTime(25),
                            rs.getTime(13)to rs.getTime(26),
                        )
                    }
                }
            }
            checkEquality(result)
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

            val result: Array<Pair<Any?, Any?>> = assertDoesNotThrow {
                c.prepareStatement(compositeCompareQuery).use { s ->
                    s.setObject(
                        1,
                        PGobject().apply {
                            type = "complex_composite_test"
                            value = compositeLiteral
                        },
                    )
                    s.executeQuery().use { rs ->
                        rs.next()
                        arrayOf(
                            rs.getString(1)to rs.getString(6),
                            rs.getInt(2)to rs.getInt(7),
                            rs.getObject(3) to rs.getObject(8),
                            rs.getArray(4)to rs.getArray(9),
                            rs.getArray(5)to rs.getArray(10),
                        )
                    }
                }
            }
            checkEquality(result)
        }
    }
}
