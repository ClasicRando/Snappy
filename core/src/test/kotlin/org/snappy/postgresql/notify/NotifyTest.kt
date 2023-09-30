package org.snappy.postgresql.notify

import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.jdbc.PgConnection
import org.snappy.execute.execute
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class NotifyTest {
    private val missingEnvironmentVariableMessage = "To run Postgres tests the environment " +
            "variable SNAPPY_PG_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (PgConnection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_PG_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString).use {
            action(it.unwrap(PgConnection::class.java))
        }
    }

    @Test
    fun `notify should fail when invalid channel`() {
        val channelName = "9test_channel"
        useConnection {
            assertThrows<IllegalArgumentException> { it.notify(channelName) }
        }
    }

    @Test
    fun `notify should send empty notification when valid channel name without message`() {
        val channelName = "test_channel"
        useConnection { c ->
            c.execute("LISTEN $channelName")
            useConnection {
                it.notify(channelName)
            }
            val notifications = c.notifications
            assertTrue(notifications.isNotEmpty())
            assertTrue(notifications.first().parameter.isEmpty())
        }
    }

    @Test
    fun `notify should send notification when valid channel name with message`() {
        val channelName = "test_channel"
        val message = "test"
        useConnection { c ->
            c.execute("LISTEN $channelName")
            useConnection {
                it.notify(channelName, message)
            }
            val notifications = c.notifications
            assertTrue(notifications.isNotEmpty())
            assertNotNull(notifications.first().parameter)
            assertEquals(message, notifications.first().parameter)
        }
    }
}
