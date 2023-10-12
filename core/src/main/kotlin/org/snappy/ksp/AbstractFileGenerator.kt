package org.snappy.ksp

abstract class AbstractFileGenerator : FileGenerator {
    private val imports = mutableSetOf<String>()

    protected fun addImport(import: String) {
        if (import.startsWith("kotlin.")) return
        imports += import
    }

    protected val importsSorted: List<String> get() = imports.sorted()
}