package org.snappy.postgresql.listen

fun validateChannelName(name: String) {
    require(!name.matches(Regex("^[a-z][a-z0-9_]+$", RegexOption.IGNORE_CASE))) {
        "Listen Channel name must be valid identifier"
    }
}
