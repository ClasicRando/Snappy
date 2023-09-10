package org.snappy

import org.junit.jupiter.api.assertThrows
import org.snappy.data.SimpleTestClass
import org.snappy.extensions.query
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class QueryTest {

    private val testDbPath = "test.db"

    private inline fun useConnection(action: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite:test.db").use(action)
    }

    @BeforeTest
    fun setUp() {
        File(testDbPath).createNewFile()
    }

    @Test
    fun `query should return sequence when valid query for row type`() {
        val rowCount = 10000
        val textValue = "Test"
        val intValue = 1
        val query = """
            WITH RECURSIVE cnt(x) AS (
                SELECT 1
                UNION ALL
                SELECT x+1 FROM cnt
                LIMIT ?
            )
            SELECT c.x AS longField, r.stringField, r.intField
            FROM cnt c
            CROSS JOIN (
                SELECT ? AS stringField, ? AS intField
            ) r
        """.trimIndent()
        useConnection { connection ->
            val result = connection.query<SimpleTestClass>(
                query,
                listOf(rowCount, textValue, intValue),
            ).toList()

            assertEquals(rowCount, result.size)
            val row = result.first()
            assertEquals(textValue, row.stringField)
            assertEquals(intValue, row.intField)
            assertEquals(1L, row.longField)
            // Default Values
            assertEquals(false, row.booleanField)
            assertEquals(0, row.shortField)
            assertEquals(0.0, row.doubleField)
            assertEquals(0.0F, row.floatField)
            assertNull(row.nullField)
        }
    }

    @Test
    fun `query should return empty sequence when valid query with no rows`() {
        val textValue = "Test"
        val intValue = 1
        val query = "SELECT * FROM (SELECT ? AS stringField, ? AS intField) t WHERE 1 = 2"
        useConnection { connection ->
            val result = connection.query<SimpleTestClass>(
                query,
                listOf(textValue, intValue),
            )

            assertTrue(result.none())
        }
    }

    @Test
    fun `query should fail when connection closed`() {
        val textValue = "Test"
        val intValue = 1
        val query = "SELECT * FROM (SELECT ? AS stringField, ? AS intField) t WHERE 1 = 2"
        useConnection { connection ->
            connection.close()
            assertThrows<IllegalStateException> {
                connection.query<SimpleTestClass>(
                    query,
                    listOf(textValue, intValue),
                ).toList()
            }
        }
    }

    @AfterTest
    fun tearDown() {
        File(testDbPath).delete()
    }
}