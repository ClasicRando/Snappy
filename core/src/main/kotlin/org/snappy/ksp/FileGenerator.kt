package org.snappy.ksp

import com.google.devtools.ksp.processing.CodeGenerator

interface FileGenerator {
    fun generateFile(codeGenerator: CodeGenerator)
}
