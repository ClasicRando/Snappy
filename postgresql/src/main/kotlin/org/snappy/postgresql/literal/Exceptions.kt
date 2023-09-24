package org.snappy.postgresql.literal

import kotlin.reflect.KClass

class MissingParseType(kClass: KClass<*>)
    : Throwable("Type specified for literal parsing is not supported, '${kClass.qualifiedName}'")

class LiteralParseError @PublishedApi internal constructor(
    expectedType: String,
    value: Any?,
    reason: String? = null,
) : Throwable(
    "Error parsing composite value. Expected type $expectedType but got '$value'.${reason ?: ""}"
)

class ExhaustedBuffer internal constructor() : Throwable("Action called on exhausted literal buffer")
