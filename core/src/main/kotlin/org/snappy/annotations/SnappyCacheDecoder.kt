package org.snappy.annotations

/**
 * Attach this annotation to a class that you want to be included in an auto-population of the
 * [DecoderCache][org.snappy.decode.DecoderCache] during cache initialization. This annotation can
 * be applied to classes that:
 * - follow the rules for a default [Decoder][org.snappy.decode.Decoder] to be generated
 * - implement [Decoder][org.snappy.decode.Decoder] for another type
 * - contain a companion object that implements [Decoder][org.snappy.decode.Decoder]
 *
 * During cache initialization of [SnappyMapper][org.snappy.SnappyMapper], metadata will be
 * collected on classes with this annotation to properly cache the
 * [Decoder][org.snappy.decode.Decoder] for usage within the application lifetime.
 */
@Target(AnnotationTarget.CLASS)
annotation class SnappyCacheDecoder
