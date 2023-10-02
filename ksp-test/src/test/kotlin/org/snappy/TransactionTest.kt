package org.snappy

import org.junit.jupiter.api.assertThrows
import org.snappy.data.ResultPair
import org.snappy.execute.execute
import org.snappy.extensions.asTransaction
import org.snappy.query.querySingle
import org.snappy.query.querySingleOrNull
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class TransactionTest {

    private val testDbPath = "test.db"
    private val testDataTable = "test_transaction_table"

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
                    )
                """.trimIndent())
                statement.execute("delete from $testDataTable")
            }
        }
    }

    @Test
    fun `asTransaction should commit changes when successful operation`() {
        val id = 1
        val value = "Test Value"
        useConnection { connection ->
            val insertCount = connection.asTransaction {
                this.execute(
                    "insert into $testDataTable(id, value) values(?, ?)",
                    listOf(id, value),
                )
            }
            assertEquals(1, insertCount)

            val record = connection.querySingle<ResultPair>(
                "select id as first, value as second from $testDataTable where id = ?",
                listOf(id),
            )
            assertEquals(id, record.first)
            assertEquals(value, record.second)
        }
    }

    @Test
    fun `asTransaction should rollback changes when unsuccessful operation`() {
        val id = 1
        val value = "Test Value"
        useConnection { connection ->
            assertThrows<SQLException> {
                connection.asTransaction {
                    this.execute(
                        "insert into $testDataTable(id, value) values(?, ?)",
                        listOf(id, value),
                    )
                    this.execute(
                        "insert into $testDataTable(id, value) values(?, ?)",
                        listOf(id, value),
                    )
                }
            }

            val record = connection.querySingleOrNull<ResultPair>(
                "select id as first, value as second from $testDataTable where id = ?",
                listOf(id),
            )
            assertNull(record)
        }
    }

    @AfterTest
    fun tearDown() {
        File(testDbPath).delete()
    }
}