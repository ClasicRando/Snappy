package org.snappy

import org.snappy.data.AnnotatedTestClass
import org.snappy.data.CacheMissClass
import org.snappy.data.CompanionObjectParser
import org.snappy.data.RowClass
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RowParserCacheTest {
    @Test
    fun `insertOrReplace should populate cache when valid parser class`() {
        RowParserCache.insertOrReplace<CompanionObjectParser>(CompanionObjectParser.Companion)
    }

    @Test
    fun `loadAutoCacheClasses caches SnappyAutoCache classes`() {
        RowParserCache.loadAutoCacheClasses("org.snappy")

        assertTrue(RowParserCache.getOrNull<AnnotatedTestClass>() != null)
        assertTrue(RowParserCache.getOrNull<RowClass>() != null)
        assertTrue(RowParserCache.getOrNull<CompanionObjectParser>() != null)
    }

    @Test
    fun `getOrDefault should populate cache when no parser present`() {
        val existingParser = RowParserCache.getOrNull<CacheMissClass>()
        assertNull(existingParser)

        RowParserCache.getOrDefault<CacheMissClass>()

        val populatedCache = RowParserCache.getOrNull<CacheMissClass>()
        assertNotNull(populatedCache)
    }
}
