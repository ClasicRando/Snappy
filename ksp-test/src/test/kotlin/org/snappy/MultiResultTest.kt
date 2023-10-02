package org.snappy

import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.snappy.data.SimpleTestClass
import org.snappy.query.queryMultiple
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@EnabledIfEnvironmentVariable(named = "SNAPPY_MSSQL_TEST", matches = "true")
class MultiResultTest {

    private val missingEnvironmentVariableMessage = "To run MultiResultTest the environment " +
            "variable SNAPPY_MSSQL_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (Connection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_MSSQL_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString).use(action)
    }

    @Test
    fun `readNext should return sequence when valid multi result query`() {
        val query = "SELECT 1 AS intField; SELECT 'test' AS stringField;"
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            val result1 = reader.readNext<SimpleTestClass>().toList()
            assertTrue(result1.isNotEmpty())
            val result2 = reader.readNext<SimpleTestClass>().toList()
            assertTrue(result2.isNotEmpty())
        }
    }

    @Test
    fun `readNext should return empty sequence when a result is empty`() {
        val query = "SELECT * FROM (SELECT 1 AS intField) t WHERE 1 = 2;"
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            val result = reader.readNext<SimpleTestClass>().toList()
            assertTrue(result.isEmpty())
        }
    }

    @Test
    fun `readNextSingleOrNull should return single row when valid multi result query`() {
        val query = "SELECT 1 AS intField; SELECT 'test' AS stringField;"
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            val result1 = reader.readNextSingleOrNull<SimpleTestClass>()
            assertNotNull(result1)
            val result2 = reader.readNextSingleOrNull<SimpleTestClass>()
            assertNotNull(result2)
        }
    }

    @Test
    fun `readNextSingleOrNull should return null when a result is empty`() {
        val query = "SELECT * FROM (SELECT 1 AS intField) t WHERE 1 = 2;"
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            val result = reader.readNextSingleOrNull<SimpleTestClass>()
            assertNull(result)
        }
    }

    @Test
    fun `readNextSingleOrNull should fail when a result contains multiple rows`() {
        val query = """
            SELECT t.intField, v.longField
            FROM (SELECT 1 AS intField) t
            CROSS JOIN (VALUES(1),(2)) v(longField)
        """.trimIndent()
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            assertThrows<TooManyRows> { reader.readNextSingleOrNull<SimpleTestClass>() }
        }
    }

    @Test
    fun `readNextSingle should fail when a result is empty`() {
        val query = "SELECT * FROM (SELECT 1 AS intField) t WHERE 1 = 2;"
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            assertThrows<EmptyResult> { reader.readNextSingle<SimpleTestClass>() }
        }
    }




    @Test
    fun `readNextFirstOrNull should return single row when valid multi result query`() {
        val query = "SELECT 1 AS intField; SELECT 'test' AS stringField;"
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            val result1 = reader.readNextFirstOrNull<SimpleTestClass>()
            assertNotNull(result1)
            val result2 = reader.readNextFirstOrNull<SimpleTestClass>()
            assertNotNull(result2)
        }
    }

    @Test
    fun `readNextFirstOrNull should return null when a result is empty`() {
        val query = "SELECT * FROM (SELECT 1 AS intField) t WHERE 1 = 2;"
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            val result = reader.readNextFirstOrNull<SimpleTestClass>()
            assertNull(result)
        }
    }

    @Test
    fun `readNextFirstOrNull should return first row when a result contains multiple rows`() {
        val query = """
            SELECT t.intField, v.longField
            FROM (SELECT 1 AS intField) t
            CROSS JOIN (VALUES(1),(2)) v(longField)
        """.trimIndent()
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            val result = reader.readNextFirstOrNull<SimpleTestClass>()
            assertNotNull(result)
            assertEquals(1, result.intField)
            assertEquals(1, result.longField)
        }
    }

    @Test
    fun `readNextFirst should fail when a result is empty`() {
        val query = "SELECT * FROM (SELECT 1 AS intField) t WHERE 1 = 2;"
        useConnection { connection ->
            val reader = connection.queryMultiple(query)
            assertThrows<EmptyResult> { reader.readNextFirst<SimpleTestClass>() }
        }
    }
}