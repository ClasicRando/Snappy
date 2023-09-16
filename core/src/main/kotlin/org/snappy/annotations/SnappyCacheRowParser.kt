package org.snappy.annotations

/**
 * Attach this annotation to a class that you want to be included in an auto-population of the
 * [RowParserCache][org.snappy.rowparse.RowParserCache] during cache initialization. This annotation
 * must only be applied to classes that follow the rules for a default
 * [RowParser][org.snappy.rowparse.RowParser] to be generated (i.e. the class must not already
 * implement the [RowParser][org.snappy.rowparse.RowParser] interface either through the class
 * itself or the companion object).
 *
 * During cache initialization of [SnappyMapper][org.snappy.SnappyMapper], metadata will be
 * collected on classes with this annotation to properly cache the
 * [RowParser][org.snappy.rowparse.RowParser] for usage within the application lifetime.
 */
@Target(AnnotationTarget.CLASS)
annotation class SnappyCacheRowParser
