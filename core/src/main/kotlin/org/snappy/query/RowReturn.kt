package org.snappy.query

/**
 * Row return variants. Tells the result parser if an error should be thrown if multiple rows are
 * returned.
 */
@PublishedApi
internal enum class RowReturn {
    Single,
    First,
}