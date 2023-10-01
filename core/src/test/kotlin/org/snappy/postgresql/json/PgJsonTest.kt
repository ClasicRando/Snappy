package org.snappy.postgresql.json

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.jdbc.PgConnection
import org.snappy.postgresql.data.JsonTest
import org.snappy.query.queryScalar
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class PgJsonTest {
    private val missingEnvironmentVariableMessage = "To run Postgres tests the environment " +
            "variable SNAPPY_PG_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (PgConnection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_PG_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString).use {
            action(it.unwrap(PgConnection::class.java))
        }
    }

    @Test
    fun `PgJson should be readable from a query result`() {
        val testJson = JsonTest("This is a test", listOf(4,5,748,34))
        useConnection { c ->
            val jsonValue = c.queryScalar<PgJson>(
                "select ?::jsonb",
                listOf(Json.encodeToString(testJson)),
            )
            val result = jsonValue.decode<JsonTest>()

            assertEquals(testJson, result)
        }
    }
}
