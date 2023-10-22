package org.snappy

import kotlinx.serialization.Serializable

/** Config file information deserialized */
@Serializable
data class SnappyConfig(
    val packages: MutableList<String>,
    val allowUnderscoreMatch: Boolean = false,
)
