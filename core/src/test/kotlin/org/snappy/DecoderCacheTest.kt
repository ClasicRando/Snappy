package org.snappy

import org.snappy.data.CacheMissDecoderClass
import org.snappy.data.DecoderClass
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
        decoderCache.insertOrReplace<RowClass>(DecoderClass())
    }

    @Test
    fun `rowParserCache should be auto populated with auto cache classes`() {
        decoderCache.loadCache()
        assertTrue(decoderCache.cacheLoaded)

        val populatedCache = decoderCache.getOrNull<Instant>()
        assertNotNull(populatedCache)
    }

    @Test
    fun `getOrDefault should populate cache when no parser present`() {
        val existingParser = decoderCache.getOrNull<CacheMissDecoderClass>()
        assertNull(existingParser)

        decoderCache.getOrDefault<CacheMissDecoderClass>()

        val populatedCache = decoderCache.getOrNull<CacheMissDecoderClass>()
        assertNotNull(populatedCache)
    }
}