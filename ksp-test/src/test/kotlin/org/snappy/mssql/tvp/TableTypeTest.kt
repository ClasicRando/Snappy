package org.snappy.mssql.tvp

import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.snappy.mssql.tvp.types.TvpTestRowTableType
import org.snappy.query.query
import java.sql.DriverManager
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "SNAPPY_MSSQL_TEST", matches = "true")
class TableTypeTest {
    private val missingEnvironmentVariableMessage = "To run SQL Server tests the " +
            "environment variable SNAPPY_MSSQL_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (ISQLServerConnection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_MSSQL_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString)
            .unwrap(ISQLServerConnection::class.java)
            .use(action)
    }

    private fun readResource(name: String): String {
        return this::class.java
            .classLoader
            .getResource(name)
            ?.openStream()?.use { stream ->
                stream.bufferedReader().readText()
            } ?: throw IllegalStateException("Could not find '$name'")
    }

    @BeforeTest
    fun setup() {
        val createTvpTypeScript = readResource("start_tvp_test.sql")
        val startupScript = readResource("create_tvp_procedure.sql")
        useConnection { c ->
            c.createStatement().use { s ->
                s.execute(createTvpTypeScript)
                s.execute(startupScript)
            }
        }
    }

    @Test
    fun `encode table type should succeed for implementation of AbstractTableType`() {
        val rows = (1..10_00).map { TvpTestRow.random() }
        val testTableType = TvpTestRowTableType(rows)
        useConnection {  c ->
            val result = c.query<TvpTestRow>(
                "EXEC tvp_test_procedure ?",
                listOf(testTableType),
            ).toList()
            assertEquals(rows.size, result.size)
            for ((expected, actual) in rows.zip(result)) {
                assertEquals(expected, actual)
            }
        }
    }
}
