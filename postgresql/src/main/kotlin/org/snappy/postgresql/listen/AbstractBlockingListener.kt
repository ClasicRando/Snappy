package org.snappy.postgresql.listen

import io.github.oshai.kotlinlogging.KLogger
import org.postgresql.PGConnection
import org.postgresql.PGNotification
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/**
 * Listener on a postgres database [connection] looking for asynchronous messages on the
 * [listenChannel] specified. This object should take ownership of the [connection] meaning no other
 * threads or objects hold reference to the [connection]. Upon creation of an object instance, a new
 * [Thread] is spawned to poll the connection for new messages that may have been sent. When new
 * notifications have been received through the connection, they are passed to the
 * [processNotification] method for handling the incoming notification. When you are done using the
 * object you must [close] the listener to stop the [thread] and close the [connection].
 *
 * Implementations must provide a [KLogger] instance for logging and the action as to how
 * notifications are handled.
 */
abstract class AbstractBlockingListener<C>(
    private val connection: C,
    private val listenChannel: String,
    checkNotificationsDelay: UInt,
)  : AutoCloseable where
    C : PGConnection,
    C : Connection
{
    /** Logger for the listener */
    protected abstract val log: KLogger
    /**
     * Thread safe flag to indicate that the listener is still active. Initialized as true, but
     * later set of false when the listener is closing to exit the main loop of the listener.
     */
    private val running = AtomicBoolean(true)
    /** Delay in milliseconds between polls of the notifications sent to a connection */
    private val checkNotificationsDelay = checkNotificationsDelay.toLong()
    /** Thread spawn to handle the notification polling */
    private val thread = thread(start = true) {
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

    /**
     * Polls the [connection] for new [notifications][PGConnection.getNotifications], processing
     * with the [processNotification] method. After checking for notifications, the thread sleeps
     * for the specified duration before checking again. Loops until [running] is set to false.
     */
    private fun listen() {
        while (running.get()) {
            try {
                require(!connection.isClosed) { "Cannot listen to a closed connection" }
                connection.notifications?.let { pgNotifications ->
                    for (notification in pgNotifications) {
                        log.atInfo {
                            message = "Received a notification"
                            payload = mapOf(
                                "Name" to notification.name,
                                "PID" to notification.pid,
                                "Parameter" to notification.parameter,
                            )
                        }
                        processNotification(notification)
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

    /** Process a received [notification]. Called on each notification received by a listener. */
    protected abstract fun processNotification(notification: PGNotification)

    override fun close() {
        running.set(false)
        thread.join(checkNotificationsDelay + 100)
        if (thread.isAlive) {
            thread.interrupt()
        }
    }
}