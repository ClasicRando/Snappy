package org.snappy

import org.snappy.batch.batchSizeOrDefault
import kotlin.test.Test
import kotlin.test.assertEquals

class ExecuteBatchTest {

    @Test
    fun `batchSizeOrDefault should return input value when valid timeout`() {
        val timeout = 10u

        val result = batchSizeOrDefault(timeout)

        assertEquals(10, result)
    }

    @Test
    fun `batchSizeOrDefault should return default value when null timeout`() {
        val timeout = null

        val result = batchSizeOrDefault(timeout)

        assertEquals(100, result)
    }

    @Test
    fun `batchSizeOrDefault should return default value when 0 timeout`() {
        val timeout = 0u

        val result = batchSizeOrDefault(timeout)

        assertEquals(100, result)
    }
}