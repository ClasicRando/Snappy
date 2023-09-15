package org.snappy

import org.junit.jupiter.api.assertThrows
import org.snappy.data.SimpleTestClass
import org.snappy.query.queryFirst
import org.snappy.query.queryFirstOrNull
import org.snappy.query.querySingle
import org.snappy.query.querySingleOrNull
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class QueryOneTest {

    private val testDbPath = "test.db"

    private inline fun useConnection(action: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite:test.db").use(action)
    }

    @BeforeTest
    fun setUp() {
        File(testDbPath).createNewFile()
    }

    @Test
    fun `querySingleOrNull should return single row when valid query`() {
        val textValue = "Test"
        val intValue = 1
        val query = "SELECT ? AS stringField, ? AS intField"
        useConnection { connection ->
            val row = connection.querySingleOrNull<SimpleTestClass>(
                query,
                listOf(textValue, intValue),
            )

            assertNotNull(row)
            assertEquals(textValue, row.stringField)
            assertEquals(intValue, row.intField)
            // Default Values
            assertEquals(false, row.booleanField)
            assertEquals(0, row.shortField)
            assertEquals(0L, row.longField)
            assertEquals(0.0, row.doubleField)
            assertEquals(0.0F, row.floatField)
            assertNull(row.nullField)
        }
    }

    @Test
    fun `querySingleOrNull should return single row when empty result set`() {
        val textValue = "Test"
        val intValue = 1
        val query = "SELECT * FROM (SELECT ? AS stringField, ? AS intField) t WHERE 1 = 2"
        useConnection { connection ->
            val row = connection.querySingleOrNull<SimpleTestClass>(
                query,
                listOf(textValue, intValue),
            )

            assertNull(row)
        }
    }

    @Test
    fun `querySingleOrNull should fail when result has more than 1 row`() {
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
            assertThrows<TooManyRows> {
                connection.querySingleOrNull<SimpleTestClass>(
                    query,
                    listOf(rowCount, textValue, intValue),
                )
            }
        }
    }

    @Test
    fun `querySingle should fail when empty result set`() {
        val textValue = "Test"
        val intValue = 1
        val query = "SELECT * FROM (SELECT ? AS stringField, ? AS intField) t WHERE 1 = 2"
        useConnection { connection ->
            assertThrows<EmptyResult> {
                connection.querySingle<SimpleTestClass>(
                    query,
                    listOf(textValue, intValue),
                )
            }
        }
    }




    @Test
    fun `queryFirstOrNull should return single row when valid query`() {
        val textValue = "Test"
        val intValue = 1
        val query = "SELECT ? AS stringField, ? AS intField"
        useConnection { connection ->
            val row = connection.queryFirstOrNull<SimpleTestClass>(
                query,
                listOf(textValue, intValue),
            )

            assertNotNull(row)
            assertEquals(textValue, row.stringField)
            assertEquals(intValue, row.intField)
            // Default Values
            assertEquals(false, row.booleanField)
            assertEquals(0, row.shortField)
            assertEquals(0L, row.longField)
            assertEquals(0.0, row.doubleField)
            assertEquals(0.0F, row.floatField)
            assertNull(row.nullField)
        }
    }

    @Test
    fun `queryFirstOrNull should return single row when empty result set`() {
        val textValue = "Test"
        val intValue = 1
        val query = "SELECT * FROM (SELECT ? AS stringField, ? AS intField) t WHERE 1 = 2"
        useConnection { connection ->
            val row = connection.queryFirstOrNull<SimpleTestClass>(
                query,
                listOf(textValue, intValue),
            )

            assertNull(row)
        }
    }

    @Test
    fun `queryFirstOrNull should return single row when result has more than 1 row`() {
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
            val row = connection.queryFirstOrNull<SimpleTestClass>(
                query,
                listOf(rowCount, textValue, intValue),
            )

            assertNotNull(row)
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
    fun `queryFirst should fail when empty result set`() {
        val textValue = "Test"
        val intValue = 1
        val query = "SELECT * FROM (SELECT ? AS stringField, ? AS intField) t WHERE 1 = 2"
        useConnection { connection ->
            assertThrows<EmptyResult> {
                connection.queryFirst<SimpleTestClass>(
                    query,
                    listOf(textValue, intValue),
                )
            }
        }
    }

    @AfterTest
    fun tearDown() {
        File(testDbPath).delete()
    }
}