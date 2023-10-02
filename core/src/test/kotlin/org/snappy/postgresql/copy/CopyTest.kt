package org.snappy.postgresql.copy

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.jdbc.PgConnection
import org.snappy.execute.execute
import org.snappy.postgresql.data.CsvRow
import org.snappy.query.queryScalar
import java.io.InputStream
import java.sql.DriverManager
import kotlin.random.Random
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class CopyTest {
    private val missingEnvironmentVariableMessage = "To run Postgres tests the environment " +
            "variable SNAPPY_PG_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (PgConnection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_PG_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString).use {
            action(it.unwrap(PgConnection::class.java))
        }
    }

    private fun getCsvTestData(): InputStream {
        return this::class.java
            .classLoader
            .getResource("test.csv")
            ?.openStream()
            ?: throw IllegalStateException("Could not find 'test.csv'")
    }

    private fun readStartScript(): String {
        return this::class.java
            .classLoader
            .getResource("start_copy_test.pgsql")
            ?.openStream()?.use { stream ->
                stream.bufferedReader().readText()
            } ?: throw IllegalStateException("Could not find 'start_copy_test.pgsql'")
    }

    private val random = Random(System.currentTimeMillis())

    @BeforeTest
    fun setup() {
        val startupScript = readStartScript()
        useConnection { c ->
            c.createStatement().use { s ->
                s.execute(startupScript)
            }
        }
    }

    @Test
    fun `copyIn should copy all records when input stream`() {
        useConnection { c ->
            c.execute("TRUNCATE TABLE copy_test")
            c.copyIn(
                getCsvTestData(),
                tableName = "copy_test",
                header = true,
                columNames = listOf(
                    "bool_field",
                    "smallint_field",
                    "int_field",
                    "bigint_field",
                    "real_field",
                    "double_field",
                    "text_field",
                    "numeric_field",
                    "date_field",
                    "timestamp_field",
                    "timestamptz_field",
                    "time_field",
                    "timetz_field",
                ),
            )
            val count = c.queryScalar<Long>("SELECT count(*) FROM copy_test")
            assertEquals(100_000, count)
        }
    }

    @Test
    fun `copyIn should copy all records when sequence of ToObjectRow`() {
        val numberOfRecords = random.nextLong(0, 500_000)
        useConnection { c ->
            c.execute("TRUNCATE TABLE copy_test")
            val sequence = (1..numberOfRecords).asSequence().map { CsvRow.random() }
            c.copyInRow(
                sequence,
                tableName = "copy_test",
                header = false,
                columNames = listOf(
                    "bool_field",
                    "smallint_field",
                    "int_field",
                    "bigint_field",
                    "real_field",
                    "double_field",
                    "text_field",
                    "numeric_field",
                    "date_field",
                    "timestamp_field",
                    "timestamptz_field",
                    "time_field",
                    "timetz_field",
                ),
            )
            val count = c.queryScalar<Long>("SELECT count(*) FROM copy_test")
            assertEquals(numberOfRecords, count)
        }
    }

    @Test
    fun `copyIn should copy all records when sequence of ToCsvRow`() {
        val numberOfRecords = random.nextLong(0, 500_000)
        useConnection { c ->
            c.execute("TRUNCATE TABLE copy_test")
            val sequence = (1..numberOfRecords).asSequence().map { CsvRow.random() }
            c.copyInCsv(
                sequence,
                tableName = "copy_test",
                header = false,
                columNames = listOf(
                    "bool_field",
                    "smallint_field",
                    "int_field",
                    "bigint_field",
                    "real_field",
                    "double_field",
                    "text_field",
                    "numeric_field",
                    "date_field",
                    "timestamp_field",
                    "timestamptz_field",
                    "time_field",
                    "timetz_field",
                ),
            )
            val count = c.queryScalar<Long>("SELECT count(*) FROM copy_test")
            assertEquals(numberOfRecords, count)
        }
    }

    @Test
    fun `copyIn should copy all records when sequence builder of ToObjectRow`() {
        val numberOfRecords = random.nextLong(0, 500_000)
        useConnection { c ->
            c.execute("TRUNCATE TABLE copy_test")
            c.copyInRow(
                tableName = "copy_test",
                header = false,
                columNames = listOf(
                    "bool_field",
                    "smallint_field",
                    "int_field",
                    "bigint_field",
                    "real_field",
                    "double_field",
                    "text_field",
                    "numeric_field",
                    "date_field",
                    "timestamp_field",
                    "timestamptz_field",
                    "time_field",
                    "timetz_field",
                ),
            ) {
                for (i in 1..numberOfRecords) {
                    yield(CsvRow.random())
                }
            }
            val count = c.queryScalar<Long>("SELECT count(*) FROM copy_test")
            assertEquals(numberOfRecords, count)
        }
    }

    @Test
    fun `copyIn should copy all records when sequence builder of ToCsvRow`() {
        val numberOfRecords = random.nextLong(0, 500_000)
        useConnection { c ->
            c.execute("TRUNCATE TABLE copy_test")
            c.copyInCsv(
                tableName = "copy_test",
                header = false,
                columNames = listOf(
                    "bool_field",
                    "smallint_field",
                    "int_field",
                    "bigint_field",
                    "real_field",
                    "double_field",
                    "text_field",
                    "numeric_field",
                    "date_field",
                    "timestamp_field",
                    "timestamptz_field",
                    "time_field",
                    "timetz_field",
                ),
            ) {
                for (i in 1..numberOfRecords) {
                    yield(CsvRow.random())
                }
            }
            val count = c.queryScalar<Long>("SELECT count(*) FROM copy_test")
            assertEquals(numberOfRecords, count)
        }
    }
}
