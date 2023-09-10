package org.snappy

/**
 * Attached this annotation to a class that you want to be included in an auto-population of the
 * [RowParserCache] using [RowParserCache.loadAutoCacheClasses]. This annotation can be applied to
 * classes that:
 * - follow the rules for a default [RowParser] to be generated
 * - implement [RowParser] for another type
 * - contain a companion object that implements [RowParser]
 *
 * During a call to [RowParserCache.loadAutoCacheClasses], metadata will be collected on classes
 * with this annotation to properly cache the [RowParser] for usage within the application lifetime.
 */
@Target(AnnotationTarget.CLASS)
annotation class SnappyAutoCache
