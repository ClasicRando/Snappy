package org.snappy.logging

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging

fun <R : Any> R.logger(): Lazy<KLogger> {
    return lazy { KotlinLogging.logger {} }
}