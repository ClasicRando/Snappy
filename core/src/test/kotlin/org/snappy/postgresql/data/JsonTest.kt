package org.snappy.postgresql.data

import kotlinx.serialization.Serializable

@Serializable
data class JsonTest(val value: String, val numbers: List<Int>)
