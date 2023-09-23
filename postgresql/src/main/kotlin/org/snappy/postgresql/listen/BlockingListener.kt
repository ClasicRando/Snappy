package org.snappy.postgresql.listen

import org.postgresql.PGConnection
import org.postgresql.PGNotification
import org.snappy.logging.logger
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class BlockingListener<C>(
    private val connection: C,
    private val listenChannel: String,
    buffer: UInt = 100u,
    checkNotificationsDelay: UInt = 100u,
) where
    C : PGConnection,
    C : Connection
{
    private val log by logger()
    init {
        require(!connection.isClosed) { "Cannot listen to a closed connection" }
        validateChannelName(listenChannel)
    }
    private val blockingQueue: BlockingQueue<PGNotification> = ArrayBlockingQueue(buffer.toInt())
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
                        blockingQueue.put(notification)
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

    fun receive(
        timeout: ULong = 500u,
        timeUnit: TimeUnit = TimeUnit.MILLISECONDS,
    ): PGNotification? {
        return blockingQueue.poll(timeout.toLong(), timeUnit)
    }
}
