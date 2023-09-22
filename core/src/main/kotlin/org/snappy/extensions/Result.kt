package org.snappy.extensions

fun <T> Result<T>.mapError(func: (Throwable) -> Throwable): Result<T> {
    val exception = this.exceptionOrNull() ?: return this
    return Result.failure(func(exception))
}
