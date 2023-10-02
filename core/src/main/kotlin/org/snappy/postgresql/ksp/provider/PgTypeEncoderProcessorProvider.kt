package org.snappy.postgresql.ksp.provider

import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import org.snappy.postgresql.ksp.processor.PgTypeEncoderProcessor

class PgTypeEncoderProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor {
        return PgTypeEncoderProcessor(
            environment.codeGenerator,
            environment.logger,
        )
    }
}
