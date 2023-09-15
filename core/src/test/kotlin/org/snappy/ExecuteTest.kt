package org.snappy

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.snappy.execute.execute
import org.snappy.execute.executeOutParameters
import org.snappy.query.query
import org.snappy.query.querySingle
import org.snappy.query.querySingleOrNull
import org.snappy.statement.SqlParameter
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ExecuteTest {

    private val missingEnvironmentVariableMessage = "To run MultiResultTest the environment " +
            "variable SNAPPY_MSSQL_CONNECTION_STRING must be available"

    private val testDbPath = "test.db"
    private val testDataTable = "test_execute_data"
    private val testDataProcedure = "dbo.test_procedure"

    private inline fun useConnection(action: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite:test.db").use(action)
    }

    private inline fun useMsSqlConnection(action: (Connection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_MSSQL_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString).use(action)
    }

    @BeforeTest
    fun setUp() {
        File(testDbPath).createNewFile()
        useConnection { connection ->
            connection.createStatement().use { statement ->
                statement.execute("""
                    create table if not exists $testDataTable (
                        id integer primary key,
                        value text
                    )
                """.trimIndent())
                statement.execute("delete from $testDataTable")
                statement.execute("""
                    insert into $testDataTable (id, value)
                    values (1, 'Update Test 1'),(2, 'Update Test 2'),
                           (3, 'Delete Test 1'),(4, 'Delete Test 2')
                """.trimIndent())
            }
        }
        if (System.getenv("SNAPPY_MSSQL_CONNECTION_STRING") != null) {
            useMsSqlConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("""
                        create or alter procedure $testDataProcedure
                            @Param1 int,
                            @Param2 varchar(50) OUTPUT
                        as
                        begin
                            set @Param2 = 'Test Output';
                        end;
                    """.trimIndent())
                }
            }
        }
    }

    @Test
    fun `execute should return number of inserted records when insert command`() {
        val insertId = 5
        val insertText = "Insert Test"
        val query = """
            insert into $testDataTable(id, value)
            values (?, ?)
        """.trimIndent().trim()
        useConnection { connection ->
            val affectedCount = connection.execute(query, listOf(insertId, insertText))
            assertEquals(1, affectedCount)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2])
    fun `execute should return number of updated records when id based update command`(id: Int) {
        val updateText = "New Value"
        val updateQuery = """
            update $testDataTable
            set value = ?
            where id = ?
        """.trimIndent().trim()
        val selectQuery = "select id as first, value as second from $testDataTable where id = ?"
        useConnection { connection ->
            val affectedCount = connection.execute(updateQuery, listOf(updateText, id))
            assertEquals(1, affectedCount)

            val record = connection.querySingle<Pair<Int, String>>(
                selectQuery,
                listOf(id),
            )
            assertEquals(id, record.first)
            assertEquals(updateText, record.second)
        }
    }

    @Test
    fun `execute should return number of updated records when multi row update command`() {
        val updateText = "Multi Row Update"
        val updateQuery = """
            update $testDataTable
            set value = ?
            where value like '%Update%'
        """.trimIndent().trim()
        val selectQuery = """
            select id as first, value as second
            from $testDataTable
            where value like '%Update%'
        """.trimIndent()
        useConnection { connection ->
            val affectedCount = connection.execute(updateQuery, listOf(updateText))
            assertEquals(2, affectedCount)

            val records = connection.query<Pair<Int, String>>(selectQuery).toList()
            assertTrue(records.isNotEmpty())
            assertTrue(records.all { it.first == 1 || it.first == 2 })
            assertTrue(records.all { it.second == updateText })
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [3, 4])
    fun `execute should return number of deleted records when id based delete command`(id: Int) {
        val updateQuery = """
            delete from $testDataTable
            where id = ?
        """.trimIndent()
        val selectQuery = "select id as first, value as second from $testDataTable where id = ?"
        useConnection { connection ->
            val affectedCount = connection.execute(updateQuery, listOf(id))
            assertEquals(1, affectedCount)

            val record = connection.querySingleOrNull<Pair<Int, String>>(
                selectQuery,
                listOf(id),
            )
            assertNull(record)
        }
    }

    @EnabledIfEnvironmentVariable(named = "SNAPPY_MSSQL_TEST", matches = "true")
    @Test
    fun `executeOutParameters should return out parameters when successful`() {
        useMsSqlConnection { connection ->
            val result = connection.executeOutParameters(
                testDataProcedure,
                listOf(1, SqlParameter.Out(java.sql.Types.VARCHAR)),
            )

            assertTrue(result.isNotEmpty())
            assertEquals(1, result.size)
            val value = result[0]
            assertNotNull(value)
            assertEquals("Test Output", value)
        }
    }

    @AfterTest
    fun tearDown() {
        File(testDbPath).delete()
    }
}