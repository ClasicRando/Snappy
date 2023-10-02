package org.snappy.postgresql.ksp.provider

import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import org.snappy.postgresql.ksp.processor.PgTypeDecoderProcessor

class PgTypeDecoderProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor {
        return PgTypeDecoderProcessor(
            environment.codeGenerator,
            environment.logger,
        )
    }
}