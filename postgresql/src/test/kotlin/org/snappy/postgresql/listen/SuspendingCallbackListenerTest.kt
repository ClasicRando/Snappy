package org.snappy.postgresql.listen

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.PGNotification
import org.postgresql.jdbc.PgConnection
import org.snappy.postgresql.notify.notify
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class SuspendingCallbackListenerTest {
    private val missingEnvironmentVariableMessage = "To run Postgres tests the environment " +
            "variable SNAPPY_PG_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (PgConnection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_PG_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString).use {
            action(it.unwrap(PgConnection::class.java))
        }
    }

    private fun getConnection(): PgConnection {
        val connectionString = System.getenv("SNAPPY_PG_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        return DriverManager.getConnection(connectionString).unwrap(PgConnection::class.java)
    }

    @Test
    fun `callback should populate list when 3 notifications sent`() = runBlocking {
        val channelName = "suspend_callback_test_channel"
        val result = mutableListOf<PGNotification>()
        pgCallbackListener(getConnection(), channelName) {
            result += it
        }.use {
            useConnection { c ->
                c.notify(channelName)
                c.notify(channelName)
                c.notify(channelName)
                delay(5000)
            }
        }
        assertEquals(3, result.size)
    }
}
