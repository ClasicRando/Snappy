package org.snappy

import org.junit.jupiter.api.assertThrows
import org.snappy.query.queryScalar
import org.snappy.query.queryScalarOrNull
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class QueryScalarTest {

    private val testDbPath = "test.db"

    private inline fun useConnection(action: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite:test.db").use(action)
    }

    @BeforeTest
    fun setUp() {
        File(testDbPath).createNewFile()
    }

    @Test
    fun `queryScalar should return single value when query with non-null value`() {
        val expectedText = "Hello World"
        val query = "SELECT ? AS test"
        useConnection { connection ->
            val text = connection.queryScalar<String>(query, listOf(expectedText))
            assertEquals(expectedText, text)
        }
    }

    @Test
    fun `queryScalar should fail when query with no rows`() {
        val query = "SELECT t.test FROM (SELECT '' AS test) t WHERE 1 = 2"
        useConnection { connection ->
            assertThrows<EmptyResult> { connection.queryScalar<String>(query) }
        }
    }

    @Test
    fun `queryScalar should fail when query with null value`() {
        useConnection { connection ->
            assertThrows<EmptyResult> { connection.queryScalar<String>("SELECT null AS test") }
        }
    }

    @Test
    fun `queryScalarOrNull should return single value when query with non-null value`() {
        val expectedText = "Hello World"
        val query = "SELECT ? AS test"
        useConnection { connection ->
            val text = connection.queryScalarOrNull<String>(query, listOf(expectedText))
            assertNotNull(text)
            assertEquals(expectedText, text)
        }
    }

    @Test
    fun `queryScalarOrNull should return null when query with null value`() {
        useConnection { connection ->
            val value = connection.queryScalarOrNull<String>("SELECT null AS test")
            assertNull(value)
        }
    }

    @Test
    fun `queryScalarOrNull should return null when query with no rows`() {
        val query = "SELECT t.test FROM (SELECT '' AS test) t WHERE 1 = 2"
        useConnection { connection ->
            val value = connection.queryScalarOrNull<String>(query)
            assertNull(value)
        }
    }

    @AfterTest
    fun tearDown() {
        File(testDbPath).delete()
    }
}
