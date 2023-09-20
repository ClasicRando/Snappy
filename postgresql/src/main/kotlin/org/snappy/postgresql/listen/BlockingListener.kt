package org.snappy.postgresql.listen

import org.postgresql.PGConnection
import org.postgresql.PGNotification
import org.snappy.logging.logger
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class BlockingListener<C>(
    private val connection: C,
    private val listenChannel: String,
    buffer: UInt = 100u,
) where
    C : PGConnection,
    C : Connection
{
    private val log by logger()
    init {
        require(!connection.isClosed) {
            "Cannot listen to a closed connection"
        }
        require(!listenChannel.matches(Regex("^[a-z][a-z0-9_]+$", RegexOption.IGNORE_CASE))) {
            "Listen Channel name must be valid identifier"
        }
    }
    private val blockingQueue: BlockingQueue<PGNotification> = ArrayBlockingQueue(buffer.toInt())
    private val running = AtomicBoolean(false)

    fun start() {
        thread(start = true) {
            require(!connection.isClosed) { "Cannot listen to a closed connection" }
            connection.createStatement().use {
                it.execute("LISTEN $listenChannel")
            }
            while (running.get()) {
                try {
                    require(!connection.isClosed) { "Cannot listen to a closed connection" }
                    connection.notifications?.let { pgNotifications ->
                        for (notification in pgNotifications) {
                            blockingQueue.put(notification)
                        }
                    }
                    Thread.sleep(500)
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
            blockingQueue.clear()
        }
    }

    fun stop() {
        running.set(false)
    }

    fun receive(): PGNotification = blockingQueue.take()
}
