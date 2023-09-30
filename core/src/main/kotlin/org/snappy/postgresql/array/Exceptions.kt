package org.snappy.postgresql.array

import kotlin.reflect.KClass

class CannotEncodeArray(kClass: KClass<*>)
    : Throwable("Cannot encode List of '${kClass.qualifiedName}' into array")
