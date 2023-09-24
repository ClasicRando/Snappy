package org.snappy

import kotlinx.serialization.Serializable

/** Config file information deserialized */
@Serializable
class SnappyConfig(val packages: MutableList<String>)
