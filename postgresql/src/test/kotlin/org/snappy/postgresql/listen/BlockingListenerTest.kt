package org.snappy.postgresql.listen

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.PGNotification
import org.postgresql.jdbc.PgConnection
import org.snappy.postgresql.notify.notify
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class BlockingListenerTest {
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
    fun `queue should populate provide notifications when 3 notifications sent`() {
        val channelName = "test_channel"
        val listener = BlockingListener(getConnection(), channelName)
        useConnection { c ->
            listener.start()
            c.notify(channelName)
            Thread.sleep(500)
            c.notify(channelName)
            Thread.sleep(500)
            c.notify(channelName)
            Thread.sleep(5000)
            listener.stop()
        }
        val result = generateSequence {
            listener.receive(1u, TimeUnit.SECONDS)
        }.toList()
        assertEquals(3, result.size)
    }
}
