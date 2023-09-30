package org.snappy.postgresql.listen

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.PGNotification
import org.postgresql.jdbc.PgConnection
import org.snappy.postgresql.notify.notify
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class BlockingCallbackListenerTest {
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
    fun `callback should populate list when 3 notifications sent`() {
        val channelName = "blocking_callback_test_channel"
        val result = mutableListOf<PGNotification>()
        BlockingCallbackListener(getConnection(), channelName) {
            result += it
        }.use {
            useConnection { c ->
                Thread.sleep(500)
                c.notify(channelName)
                Thread.sleep(500)
                c.notify(channelName)
                Thread.sleep(500)
                c.notify(channelName)
                Thread.sleep(5000)
            }
        }
        assertEquals(3, result.size)
    }
}
