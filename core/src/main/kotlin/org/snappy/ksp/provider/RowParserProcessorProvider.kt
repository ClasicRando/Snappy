package org.snappy.ksp.provider

import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import org.snappy.ksp.processor.RowParserProcessor

class RowParserProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor {
        return RowParserProcessor(
            environment.codeGenerator,
            environment.logger,
        )
    }
}