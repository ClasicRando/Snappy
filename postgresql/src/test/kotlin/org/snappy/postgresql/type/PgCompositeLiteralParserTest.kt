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
                SimpleCompositeTest.decodePgObject(result)
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