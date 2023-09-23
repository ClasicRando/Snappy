package org.snappy.postgresql.listen

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.PGNotification
import org.postgresql.jdbc.PgConnection
import org.snappy.postgresql.notify.notify
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class SuspendingListenerTest {
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
    fun `queue should populate provide notifications when 3 notifications sent`() = runBlocking {
        val channelName = "suspend_test_channel"
        val result = pgListener(getConnection(), channelName).use {
            useConnection { c ->
                c.notify(channelName)
                c.notify(channelName)
                c.notify(channelName)
            }
            withTimeout(5000) {
                buildList {
                    add(it.receive())
                    add(it.receive())
                    add(it.receive())
                }
            }
        }
        assertEquals(3, result.size)
    }
}
