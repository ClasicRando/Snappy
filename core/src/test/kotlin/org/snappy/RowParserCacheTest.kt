package org.snappy

import org.snappy.data.AnnotatedTestClass
import org.snappy.data.CacheMissClass
import org.snappy.data.CompanionObjectParser
import org.snappy.rowparse.RowParserCache
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RowParserCacheTest {
    private val config = SnappyConfig(mutableListOf("org.snappy"))
    private val rowParserCache = RowParserCache(config)

    @Test
    fun `insertOrReplace should populate cache when valid parser class`() {
        rowParserCache.insertOrReplace<CompanionObjectParser>(CompanionObjectParser.Companion)
    }

    @Test
    fun `rowParserCache should be auto populated with auto cache classes`() {
        rowParserCache.loadCache()
        assertTrue(rowParserCache.cacheLoaded)

        val populatedCache = rowParserCache.getOrNull<AnnotatedTestClass>()
        assertNotNull(populatedCache)
    }

    @Test
    fun `getOrDefault should populate cache when no parser present`() {
        val existingParser = rowParserCache.getOrNull<CacheMissClass>()
        assertNull(existingParser)

        rowParserCache.getOrDefault<CacheMissClass>()

        val populatedCache = rowParserCache.getOrNull<CacheMissClass>()
        assertNotNull(populatedCache)
    }
}
