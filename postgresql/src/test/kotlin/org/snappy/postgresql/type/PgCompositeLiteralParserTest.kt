package org.snappy.postgresql.type

import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.util.PGobject
import org.snappy.postgresql.data.ComplexCompositeTest
import org.snappy.postgresql.data.SimpleCompositeTest
import org.snappy.query.queryScalar
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.BeforeTest
import kotlin.test.Test

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class PgCompositeLiteralParserTest {
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
    fun `simple composite fetched from database can be decoded`() {
        val compositeCompareQuery = readResource("simple_composite_query.pgsql")
        useConnection { c ->
            val result = assertDoesNotThrow {
                c.queryScalar<PGobject>(compositeCompareQuery)
            }
            assertDoesNotThrow {
                parseComposite(result) {
                    val boolField = readBoolean() ?: error("bool field cannot be null")
                    val smallintField = readShort() ?: error("short field cannot be null")
                    val intField = readInt() ?: error("int field cannot be null")
                    val bigintField = readLong() ?: error("long field cannot be null")
                    val realField = readFloat() ?: error("float field cannot be null")
                    val doubleField = readDouble() ?: error("double field cannot be null")
                    val textField = readString() ?: error("string field cannot be null")
                    val numericField = readBigDecimal() ?: error("numeric field cannot be null")
                    val dateField = readLocalDate() ?: error("local date field cannot be null")
                    val timestampField = readLocalDateTime()
                        ?: error("local date time field cannot be null")
                    val timestampTzField = readOffsetDateTime()
                        ?: error("offset date time field cannot be null")
                    val timeField = readLocalTime() ?: error("local time field cannot be null")
                    val timeTzField = readOffsetTime() ?: error("offset time field cannot be null")
                    SimpleCompositeTest(
                        boolField,
                        smallintField,
                        intField,
                        bigintField,
                        realField,
                        doubleField,
                        textField,
                        numericField,
                        dateField,
                        timestampField,
                        timestampTzField,
                        timeField,
                        timeTzField,
                    )
                }
            }
        }
    }

    @Test
    fun `complex composite fetched from database can be decoded`() {
        val compositeCompareQuery = readResource("complex_composite_query.pgsql")
        useConnection { c ->
            val result = assertDoesNotThrow {
                c.queryScalar<PGobject>(compositeCompareQuery)
            }
            assertDoesNotThrow {
                ComplexCompositeTest.decodePgObject(result)
            }
        }
    }
}