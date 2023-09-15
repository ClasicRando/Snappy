package org.snappy.annotations

/**
 * Attach this annotation to a class that you want to be included in an auto-population of the
 * [RowParserCache][org.snappy.rowparse.RowParserCache] during cache initialization. This annotation
 * can be applied to classes that:
 * - follow the rules for a default [RowParser][org.snappy.rowparse.RowParser] to be generated
 * - implement [RowParser][org.snappy.rowparse.RowParser] for another type
 * - contain a companion object that implements [RowParser][org.snappy.rowparse.RowParser]
 *
 * During cache initialization of [SnappyMapper][org.snappy.SnappyMapper], metadata will be
 * collected on classes with this annotation to properly cache the
 * [RowParser][org.snappy.rowparse.RowParser] for usage within the application lifetime.
 */
@Target(AnnotationTarget.CLASS)
annotation class SnappyCacheRowParser
