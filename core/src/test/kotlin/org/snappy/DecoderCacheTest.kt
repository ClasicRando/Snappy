package org.snappy

import org.snappy.data.CacheMissDecoderClass
import org.snappy.data.CompanionObjectDecoder
import org.snappy.data.RowClass
import org.snappy.decode.DecoderCache
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DecoderCacheTest {
    private val config = SnappyConfig(mutableListOf("org.snappy"))
    private val decoderCache = DecoderCache(config)

    @Test
    fun `insertOrReplace should populate cache when valid parser class`() {
        decoderCache.insertOrReplace<RowClass> { RowClass("") }
    }

    @Test
    fun `rowParserCache should be auto populated with auto cache classes`() {
        decoderCache.loadCache()
        assertTrue(decoderCache.cacheLoaded)

        assertNotNull(decoderCache.getOrNull<Instant>())
        assertNotNull(decoderCache.getOrNull<CompanionObjectDecoder>())
    }

    @Test
    fun `getOrDefault should return null when no parser present`() {
        val existingParser = decoderCache.getOrNull<CacheMissDecoderClass>()
        assertNull(existingParser)
    }
}