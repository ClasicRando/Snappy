package org.snappy.postgresql.listen

import io.github.oshai.kotlinlogging.KLogger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.postgresql.PGConnection
import org.postgresql.PGNotification
import java.sql.Connection
import java.sql.SQLException
import kotlin.coroutines.cancellation.CancellationException

/**
 * Listener on a postgres database [connection] looking for asynchronous messages on the
 * [listenChannel] specified. This object should take ownership of the [connection] meaning no other
 * threads or objects hold reference to the [connection]. Upon creation of an object instance, a new
 * [Job][kotlinx.coroutines.Job] is launched to poll the connection for new messages that may have
 * been sent. Note: The coroutine is launched within the context of [Dispatchers.IO] since polling
 * is a blocking operation. When new notifications have been received through the connection, they
 * are passed to the [processNotification] method for handling the incoming notification. When you
 * are done using the object you must [close] the listener to stop the [job] and close the
 * [connection].
 *
 * Implementations must provide a [KLogger] instance for logging and the action as to how
 * notifications are handled.
 */
abstract class AbstractSuspendingListener<C>(
    private val connection: C,
    private val listenChannel: String,
    checkNotificationsDelay: UInt,
    scope: CoroutineScope,
) where
    C : PGConnection,
    C : Connection
{
    /** Logger for the listener */
    abstract val log: KLogger
    /** Delay in milliseconds between polls of the notifications sent to a connection */
    private val checkNotificationsDelay = checkNotificationsDelay.toLong()
    /** Launched coroutine to handle notification polling */
    private val job = scope.launch(context = Dispatchers.IO) {
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
     * with the [processNotification] method. After checking for notifications, the coroutine is
     * suspended for the specified duration before checking again. Loops until an error occurs
     * or the coroutine is canceled.
     */
    private suspend fun CoroutineScope.listen() {
        while (isActive) {
            try {
                require(!connection.isClosed) { "Cannot listen to a closed connection" }
                connection.notifications?.let { pgNotifications ->
                    for (notification in pgNotifications) {
                        log.atTrace {
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
                delay(checkNotificationsDelay)
            } catch (cancel: CancellationException) {
                log.info { "Exiting listener due to CancellationException" }
                break
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
    protected abstract suspend fun processNotification(notification: PGNotification)

    /** Close the listener by canceling and joining the underlining [job]. */
    open suspend fun close() {
        job.cancelAndJoin()
    }
}