package org.snappy

import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.snappy.batch.ParameterBatch
import org.snappy.batch.executeBatch
import org.snappy.data.ResultPair
import org.snappy.query.query
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

    @ParameterizedTest
    @MethodSource("insertRecords")
    fun `executeBatch should return number of inserted records when insert command`(
        args: List<ResultPair>,
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

            val records = connection.query<ResultPair>(selectQuery).toList()
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

            val records = connection.query<ResultPair>(selectQuery).toList()
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
        fun insertRecords(): Stream<List<ResultPair>> {
            return Stream.of(
                listOf(
                    ResultPair(7, "Insert Test 1"),
                    ResultPair(8, "Insert Test 2"),
                ),
                listOf(
                    ResultPair(9, "Insert Test 3"),
                ),
                listOf(
                    ResultPair(10, "Insert Test 4"),
                    ResultPair(11, "Insert Test 5"),
                    ResultPair(12, "Insert Test 6"),
                )
            )
        }
    }
}