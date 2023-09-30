package org.snappy.postgresql.listen

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import org.postgresql.PGConnection
import org.postgresql.PGNotification
import org.snappy.logging.logger
import java.sql.Connection

/** Create a new [SuspendingListener] attached to the receiver [CoroutineScope] */
fun <C> CoroutineScope.pgListener(
    connection: C,
    listenChannel: String,
    buffer: UInt = 100u,
    checkNotificationsDelay: UInt = 100u,
): SuspendingListener<C>
where
    C : PGConnection,
    C : Connection
{
    return SuspendingListener(
        connection,
        listenChannel,
        buffer,
        checkNotificationsDelay,
        this,
    )
}

/**
 * Listener on a postgres database [connection] looking for asynchronous messages on the
 * [listenChannel] specified. This object should take ownership of the [connection] meaning no other
 * threads or objects hold reference to the [connection]. Upon creation of an object instance, a new
 * [Job][kotlinx.coroutines.Job] is launched to poll the connection for new messages that may have
 * been sent. Note: The coroutine is launched within the context of [Dispatchers.IO] since polling
 * is a blocking operation. When new notifications have been received through the connection, they
 * are sent to a [Channel] for retrieval by the [receive] and [receiveCatching] methods. When you
 * are done using the object you must [close] the listener to stop the [job], close the [channel]
 * and close the [connection].
 */
class SuspendingListener<C> internal constructor(
    connection: C,
    listenChannel: String,
    buffer: UInt,
    checkNotificationsDelay: UInt,
    scope: CoroutineScope,
) : AbstractSuspendingListener<C>(connection, listenChannel, checkNotificationsDelay, scope) where
    C : PGConnection,
    C : Connection
{
    override val log by logger()
    /** */
    private val channel = Channel<PGNotification>(buffer.toInt())

    override suspend fun processNotification(notification: PGNotification) {
        channel.send(notification)
    }

    /** */
    suspend fun receive(): PGNotification = channel.receive()
    /** */
    suspend fun receiveCatching(): ChannelResult<PGNotification> = channel.receiveCatching()

    override suspend fun close() {
        channel.close()
        super.close()
    }
}