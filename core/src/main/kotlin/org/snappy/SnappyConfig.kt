package org.snappy

import kotlinx.serialization.Serializable

@Serializable
class SnappyConfig(val basePackages: MutableList<String>)
