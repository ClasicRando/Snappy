package org.snappy.ksp.processor

data class PropertyAnnotations(
    val rename: String? = null,
    val flatten: Boolean = false,
) {
    val hasValues = rename != null || flatten
    fun merge(other: PropertyAnnotations): PropertyAnnotations {
        return PropertyAnnotations(
            other.rename ?: rename,
            other.flatten || flatten,
        )
    }

    companion object {
        val default = PropertyAnnotations()
    }
}
