package org.snappy.ksp

import com.google.devtools.ksp.symbol.KSVisitorVoid

abstract class FileGeneratorVisitor : KSVisitorVoid(), FileGenerator {
    private val imports = mutableSetOf<String>()

    protected fun addImport(import: String) {
        if (import.startsWith("kotlin.")) return
        imports += import
    }

    protected val importsSorted: List<String> get() = imports.sorted()
}