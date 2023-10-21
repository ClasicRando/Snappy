package org.snappy.logging

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging

@Suppress("UnusedReceiverParameter")
internal fun <R : Any> R.logger(): Lazy<KLogger> {
    return lazy { KotlinLogging.logger {} }
}

internal fun logger(): Lazy<KLogger> {
    return lazy { KotlinLogging.logger {} }
}
