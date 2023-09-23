package org.snappy.postgresql.listen

import org.postgresql.PGConnection
import org.postgresql.PGNotification
import org.snappy.logging.logger
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class BlockingCallbackListener<C>(
    private val connection: C,
    private val listenChannel: String,
    checkNotificationsDelay: UInt = 100u,
    private val callback: (PGNotification) -> Unit
) where
    C : PGConnection,
    C : Connection
{
    private val log by logger()
    private val running = AtomicBoolean(false)
    private val checkNotificationsDelay = checkNotificationsDelay.toLong()

    fun start() {
        running.set(true)
        thread(start = true) {
            require(!connection.isClosed) { "Cannot listen to a closed connection" }
            connection.use { c ->
                try {
                    c.listen(listenChannel)
                    listen()
                } finally {
                    c.unlisten(listenChannel)
                }
            }
        }
    }

    private fun listen() {
        while (running.get()) {
            try {
                require(!connection.isClosed) { "Cannot listen to a closed connection" }
                connection.notifications?.let { pgNotifications ->
                    for (notification in pgNotifications) {
                        callback(notification)
                    }
                }
                Thread.sleep(checkNotificationsDelay)
            } catch (sqlException: SQLException) {
                log.error(sqlException) {
                    "Encountered SQL error listening to Postgres connection"
                }
                break
            } catch (ex: Throwable) {
                log.error(ex) {
                    "Encountered unknown error listening to Postgres connection"
                }
                break
            }
        }
    }

    fun stop() {
        running.set(false)
    }
}
