package org.snappy.postgresql.listen

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import org.postgresql.PGConnection
import org.postgresql.PGNotification
import org.snappy.logging.logger
import java.sql.Connection

/** Create a new [SuspendingCallbackListener] attached to the receiver [CoroutineScope] */
fun <C> CoroutineScope.pgCallbackListener(
    connection: C,
    listenChannel: String,
    checkNotificationsDelay: UInt = 100u,
    callback: suspend (PGNotification) -> Unit,
): SuspendingCallbackListener<C>
where
    C : PGConnection,
    C : Connection
{
    return SuspendingCallbackListener(
        connection,
        listenChannel,
        checkNotificationsDelay,
        this,
        callback,
    )
}

/**
 * Listener on a postgres database [connection] looking for asynchronous messages on the
 * [listenChannel] specified. This object should take ownership of the [connection] meaning no other
 * threads or objects hold reference to the [connection]. Upon creation of an object instance, a new
 * [Job][kotlinx.coroutines.Job] is launched to poll the connection for new messages that may have
 * been sent. Note: The coroutine is launched within the context of [Dispatchers.IO] since polling
 * is a blocking operation. When new notifications have been received through the connection, the
 * [callback] method is invoked to handle the notification. When you are done using the object you
 * must [close] the listener to stop the [job] and close the [connection].
 */
class SuspendingCallbackListener<C>(
    connection: C,
    listenChannel: String,
    checkNotificationsDelay: UInt,
    scope: CoroutineScope,
    private val callback: suspend (PGNotification) -> Unit,
) : AbstractSuspendingListener<C>(connection, listenChannel, checkNotificationsDelay, scope) where
    C : PGConnection,
    C : Connection
{
    override val log by logger()

    override suspend fun processNotification(notification: PGNotification) {
        callback(notification)
    }
}
