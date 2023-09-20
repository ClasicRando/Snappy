package org.snappy.postgresql.listen

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.postgresql.PGConnection
import org.postgresql.PGNotification
import org.snappy.logging.logger
import java.sql.Connection
import java.sql.SQLException

fun <C> CoroutineScope.callbackListener(
    connection: C,
    listenChannel: String,
    callback: suspend (PGNotification) -> Unit,
): SuspendingCallbackListener<C>
where
    C : PGConnection,
    C : Connection
{
    return SuspendingCallbackListener(connection, listenChannel, this, callback)
}

class SuspendingCallbackListener<C>(
    private val connection: C,
    private val listenChannel: String,
    scope: CoroutineScope,
    private val callback: suspend (PGNotification) -> Unit,
) where
    C : PGConnection,
    C : Connection
{
    private val log by logger()
    init {
        require(!connection.isClosed) { "Cannot listen to a closed connection" }
        validateChannelName(listenChannel)
    }
    private val job = scope.launch(context = Dispatchers.IO) {
        require(!connection.isClosed) { "Cannot listen to a closed connection" }
        connection.use { c ->
            c.createStatement().use {
                it.execute("LISTEN $listenChannel")
            }
            while (isActive) {
                try {
                    require(!c.isClosed) { "Cannot listen to a closed connection" }
                    c.notifications?.let { pgNotifications ->
                        for (notification in pgNotifications) {
                            callback(notification)
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
        }
    }

    fun stop() {
        job.cancel()
    }
}
