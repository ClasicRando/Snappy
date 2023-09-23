package org.snappy.postgresql.listen

import org.postgresql.PGConnection
import java.sql.Connection

fun validateChannelName(name: String) {
    require(name.matches(Regex("^[a-z][a-z0-9_]+$", RegexOption.IGNORE_CASE))) {
        "Listen Channel name must be valid identifier"
    }
}

fun <C> C.listen(channelName: String)
where
    C : PGConnection,
    C : Connection
{
    validateChannelName(channelName)
    createStatement().use {
        it.execute("LISTEN $channelName")
    }
}

fun <C> C.unlisten(channelName: String)
        where
        C : PGConnection,
        C : Connection
{
    validateChannelName(channelName)
    createStatement().use {
        it.execute("UNLISTEN $channelName")
    }
}
