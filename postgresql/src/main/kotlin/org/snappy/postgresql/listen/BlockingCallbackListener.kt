package org.snappy.postgresql.listen

import org.postgresql.PGConnection
import org.postgresql.PGNotification
import org.snappy.logging.logger
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/**
 * Listener on a postgres database [connection] looking for asynchronous messages on the
 * [listenChannel] specified. This object should take ownership of the [connection] meaning no other
 * threads or objects hold reference to the [connection]. Upon creation of an object instance, a new
 * [Thread] is spawned to poll the connection for new messages that may have been sent. When new
 * notifications have been received through the connection, the [callback] is executed to handle the
 * notification. When you are done using the object you must [close] the listener to stop the thread
 * and close the [connection].
 *
 * By default, the delay between checking for new notifications is defaulted to 100 ms but can be
 * overridden by specifying `checkNotificationsDelay` in milliseconds
 */
class BlockingCallbackListener<C>(
    connection: C,
    listenChannel: String,
    checkNotificationsDelay: UInt = 100u,
    private val callback: (PGNotification) -> Unit
) : AbstractBlockingListener<C>(connection, listenChannel, checkNotificationsDelay) where
    C : PGConnection,
    C : Connection
{
    override val log by logger()

    override fun processNotification(notification: PGNotification) {
        callback(notification)
    }
}
