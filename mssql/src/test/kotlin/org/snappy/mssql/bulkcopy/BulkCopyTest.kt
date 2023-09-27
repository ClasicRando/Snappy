package org.snappy.mssql.bulkcopy

import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.snappy.execute.execute
import org.snappy.execute.executeSuspend
import org.snappy.mssql.tvp.TvpTestRow
import org.snappy.query.queryScalar
import org.snappy.query.queryScalarSuspend
import java.io.InputStream
import java.sql.DriverManager
import kotlin.random.Random
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "SNAPPY_MSSQL_TEST", matches = "true")
class BulkCopyTest {
    private val missingEnvironmentVariableMessage = "To run SQL Server tests the " +
            "environment variable SNAPPY_MSSQL_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (ISQLServerConnection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_MSSQL_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString)
            .unwrap(ISQLServerConnection::class.java)
            .use(action)
    }

    private fun readStartScript(): String {
        return this::class.java
            .classLoader
            .getResource("start_bulk_copy_test.sql")
            ?.openStream()?.use { stream ->
                stream.bufferedReader().readText()
            } ?: throw IllegalStateException("Could not find 'start_bulk_copy_test.sql'")
    }

    private fun getCsvFilStream(): InputStream {
        return this::class.java
            .classLoader
            .getResourceAsStream("bulk_copy_data.csv")
            ?: throw IllegalStateException("Could not find 'bulk_copy_data.csv'")
    }

    private fun getCsvFilPath(): String {
        return this::class.java
            .classLoader
            .getResource("bulk_copy_data.csv")
            ?.path
            ?: throw IllegalStateException("Could not find 'bulk_copy_data.csv'")
    }

    private val destinationTableName = "bulk_copy_table"
    private val recordCount = 100_000L

    @BeforeTest
    fun setup() {
        val createTvpTypeScript = readStartScript()
        useConnection { c ->
            c.createStatement().use { s ->
                s.execute(createTvpTypeScript)
            }
        }
    }

    @Test
    fun `bulkCopyCsvFile should copy all rows when source file path`() {
        val filePath = getCsvFilPath()
        useConnection {
            it.execute("TRUNCATE TABLE $destinationTableName")
            it.bulkCopyCsvFile(destinationTableName, filePath)
            val resultRecordCount = it.queryScalar<Long>("SELECT COUNT(*) FROM $destinationTableName")
            assertEquals(recordCount, resultRecordCount)
        }
    }

    @Test
    fun `bulkCopyCsvFile should copy all rows when source file stream`() {
        val fileStream = getCsvFilStream()
        useConnection {
            it.execute("TRUNCATE TABLE $destinationTableName")
            it.bulkCopyCsvFile(destinationTableName, fileStream)
            val resultRecordCount = it.queryScalar<Long>("SELECT COUNT(*) FROM $destinationTableName")
            assertEquals(recordCount, resultRecordCount)
        }
    }

    @Test
    fun `bulkCopySequence should copy all rows when sequence builder`() {
        val sequenceLength = Random(System.currentTimeMillis())
            .nextLong(20_000, 1_000_000)
        useConnection { c ->
            c.execute("TRUNCATE TABLE $destinationTableName")
            c.bulkCopySequence(destinationTableName) {
                for (i in 1..sequenceLength) {
                    yield(TvpTestRow.random())
                }
            }
            val resultRecordCount = c.queryScalar<Long>("SELECT COUNT(*) FROM $destinationTableName")
            assertEquals(sequenceLength, resultRecordCount)
        }
    }

    @Test
    fun `bulkCopyCsvFileSuspend should copy all rows when source file path`() = runBlocking {
        val filePath = getCsvFilPath()
        useConnection {
            it.executeSuspend("TRUNCATE TABLE $destinationTableName")
            it.bulkCopyCsvFileSuspend(destinationTableName, filePath)
            val resultRecordCount = it.queryScalarSuspend<Long>("SELECT COUNT(*) FROM $destinationTableName")
            assertEquals(recordCount, resultRecordCount)
        }
    }

    @Test
    fun `bulkCopySequenceSuspend should copy all rows when sequence builder`() = runBlocking {
        val sequenceLength = Random(System.currentTimeMillis())
            .nextLong(20_000, 1_000_000)
        useConnection {
            it.executeSuspend("TRUNCATE TABLE $destinationTableName")
            it.bulkCopySequenceSuspend(destinationTableName) {
                for (i in 1..sequenceLength) {
                    yield(TvpTestRow.random())
                }
            }
            val resultRecordCount = it.queryScalarSuspend<Long>("SELECT COUNT(*) FROM $destinationTableName")
            assertEquals(sequenceLength, resultRecordCount)
        }
    }
}