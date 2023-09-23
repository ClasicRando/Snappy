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

/**
 * Listener on a postgres database [connection] looking for asynchronous messages on the
 * [listenChannel] specified. This object should take ownership of the [connection] meaning no other
 * threads or objects hold reference to the [connection]. Upon creation of an object instance, a new
 * [Thread] is spawned to poll the connection for new messages that may have been sent. When new
 * notifications have been received through the connection, they are pushed to the [blockingQueue]
 * for retrieval using the [receive] method. When you are done using the object you must [close]
 * the listener to stop the thread and close the [connection].
 *
 * By default, the size of the blocking queue is 100 but can be overridden by specifying a value for
 * `buffer` in the constructor. Also, the delay between checking for new notifications is defaulted
 * to 100 ms but can be overridden by specifying `checkNotificationsDelay` in milliseconds
 */
class BlockingListener<C>(
    connection: C,
    listenChannel: String,
    buffer: UInt = 100u,
    checkNotificationsDelay: UInt = 100u,
) : AbstractBlockingListener<C>(connection, listenChannel, checkNotificationsDelay) where
    C : PGConnection,
    C : Connection
{
    override val log by logger()
    /** Queue holding all notifications received but not yet pulled for manual processing */
    private val blockingQueue: BlockingQueue<PGNotification> = ArrayBlockingQueue(buffer.toInt())

    override fun processNotification(notification: PGNotification) {
        log.info {
            val name = notification.name
            val pid = notification.pid
            val parameter = notification.parameter
            "Received a notification, PGNotification(Name=$name,PID=$pid,Parameter=$parameter)"
        }
        blockingQueue.put(notification)
    }

    /**
     * [poll][BlockingQueue.poll] a blocking queue for the next element. Waiting for the [timeout]
     * in the [timeUnit] specified before returning null if no elements can be pulled from the
     * queue. By default, the [timeout] is 500ms
     *
     * @exception InterruptedException
     */
    fun receive(
        timeout: ULong = 500u,
        timeUnit: TimeUnit = TimeUnit.MILLISECONDS,
    ): PGNotification? {
        return blockingQueue.poll(timeout.toLong(), timeUnit)
    }
}
