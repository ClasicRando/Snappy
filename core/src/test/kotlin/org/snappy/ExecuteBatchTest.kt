package org.snappy

import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.snappy.extensions.batchSizeOrDefault
import org.snappy.extensions.executeBatch
import org.snappy.extensions.query
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.util.stream.Stream
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ExecuteBatchTest {

    private val testDbPath = "test.db"
    private val testDataTable = "test_execute_data"

    private inline fun useConnection(action: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite:test.db").use(action)
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
                    );
                """.trimIndent())
                statement.execute("delete from $testDataTable")
                statement.execute("""
                    insert into $testDataTable (id, value)
                    values (1, 'Update Test 1'),(2, 'Update Test 2'),
                           (3, 'Delete Test 1'),(4, 'Delete Test 2');
                """.trimIndent())
            }
        }
    }

    @Test
    fun `batchSizeOrDefault should return input value when valid timeout`() {
        val timeout = 10u

        val result = batchSizeOrDefault(timeout)

        assertEquals(10, result)
    }

    @Test
    fun `batchSizeOrDefault should return default value when null timeout`() {
        val timeout = null

        val result = batchSizeOrDefault(timeout)

        assertEquals(100, result)
    }

    @Test
    fun `batchSizeOrDefault should return default value when 0 timeout`() {
        val timeout = 0u

        val result = batchSizeOrDefault(timeout)

        assertEquals(100, result)
    }

    @ParameterizedTest
    @MethodSource("insertRecords")
    fun `executeBatch should return number of inserted records when insert command`(
        args: List<Pair<Int, String>>,
    ) {
        val query = """
            insert into $testDataTable(id, value)
            values (?, ?)
        """.trimIndent()
        useConnection { connection ->
            val affectedCount = connection.executeBatch(
                query,
                args.asSequence().map { ParameterBatch { listOf(it.first, it.second) } },
            )
            assertTrue(affectedCount.all { it == 1 })
            assertEquals(args.size, affectedCount.sum())
        }
    }

    @Test
    fun `executeBatch should return number of updated records when update command`() {
        val newValue = "Updated Test"
        val updateQuery = """
            update $testDataTable
            set value = ?
            where value like '%Update%'
        """.trimIndent()
        val selectQuery = """
            select id as first, value as second
            from $testDataTable
            where value like '%Update%'
        """.trimIndent()
        useConnection { connection ->
            val count = connection.executeBatch(
                updateQuery,
                sequenceOf(ParameterBatch { listOf(newValue) }),
            )
            assertEquals(1, count.size)
            assertEquals(2, count.sum())

            val records = connection.query<Pair<Int, String>>(selectQuery).toList()
            assertTrue(records.isNotEmpty())
            assertEquals(2, records.size)
            assertTrue(records.all { it.first == 1 || it.first == 2 })
            assertTrue(records.all { it.second == newValue })
        }
    }

    @Test
    fun `executeBatch should not apply changes when failed delete command`() {
        val deleteQuery = """
            delete from $testDataTable
            where id = ?
        """.trimIndent()
        val selectQuery = """
            select id as first, value as second
            from $testDataTable
            where value like '%Delete%'
        """.trimIndent()
        useConnection { connection ->
            assertThrows<ArrayIndexOutOfBoundsException> {
                connection.executeBatch(
                    deleteQuery,
                    sequenceOf(
                        ParameterBatch { listOf(4) },
                        ParameterBatch { listOf("test", "test") }
                    ),
                )
            }

            val records = connection.query<Pair<Int, String>>(selectQuery).toList()
            assertTrue(records.isNotEmpty())
            assertEquals(2, records.size)
            assertTrue(records.all { it.first == 3 || it.first == 4 })
        }
    }

    @AfterTest
    fun tearDown() {
        File(testDbPath).delete()
    }

    companion object {
        @JvmStatic
        fun insertRecords(): Stream<List<Pair<Int, String>>> {
            return Stream.of(
                listOf(
                    7 to "Insert Test 1",
                    8 to "Insert Test 2",
                ),
                listOf(
                    9 to "Insert Test 3",
                ),
                listOf(
                    10 to "Insert Test 4",
                    11 to "Insert Test 5",
                    12 to "Insert Test 6",
                )
            )
        }
    }
}