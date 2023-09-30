package org.snappy.postgresql.literal

import kotlin.reflect.KClass

/** Exception thrown when a type specified to parse a postgres array literal is not supported */
class MissingParseType internal constructor(kClass: KClass<*>)
    : Throwable("Type specified for literal parsing is not supported, '${kClass.qualifiedName}'")

/** Exception thrown when a literal parser cannot decode a value to the expected type */
class LiteralParseError @PublishedApi internal constructor(
    expectedType: String,
    value: Any?,
    reason: String? = null,
) : Throwable(
    "Error parsing composite value. Expected type $expectedType but got '$value'.${reason ?: ""}"
)

/**
 * Exception thrown when a literal parser has exhausted all values but a parser calls read one more
 * time.
 */
class ExhaustedBuffer internal constructor() : Throwable("Action called on exhausted literal buffer")
