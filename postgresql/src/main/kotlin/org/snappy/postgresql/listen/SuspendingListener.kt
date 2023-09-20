package org.snappy.postgresql.listen

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.postgresql.PGConnection
import org.postgresql.PGNotification
import org.snappy.logging.logger
import java.sql.Connection
import java.sql.SQLException

fun <C> CoroutineScope.listener(
    connection: C,
    listenChannel: String,
    buffer: UInt = 100u,
): SuspendingListener<C>
where
    C : PGConnection,
    C : Connection
{
    return SuspendingListener(connection, listenChannel, this, buffer)
}

class SuspendingListener<C>(
    private val connection: C,
    private val listenChannel: String,
    scope: CoroutineScope,
    buffer: UInt,
) where
    C : PGConnection,
    C : Connection
{
    private val log by logger()
    init {
        require(!connection.isClosed) { "Cannot listen to a closed connection" }
        validateChannelName(listenChannel)
    }
    private val channel = Channel<PGNotification>(buffer.toInt())
    private val job = scope.launch(context = Dispatchers.IO) {
        require(!connection.isClosed) { "Cannot listen to a closed connection" }
        connection.createStatement().use {
            it.execute("LISTEN $listenChannel")
        }
        while (isActive) {
            try {
                require(!connection.isClosed) { "Cannot listen to a closed connection" }
                connection.notifications?.let { pgNotifications ->
                    for (notification in pgNotifications) {
                        channel.send(notification)
                    }
                }
                delay(500)
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
        channel.close()
    }

    fun stop() {
        job.cancel()
    }

    suspend fun receive(): PGNotification = channel.receive()
    suspend fun receiveCatching(): ChannelResult<PGNotification> = channel.receiveCatching()
}