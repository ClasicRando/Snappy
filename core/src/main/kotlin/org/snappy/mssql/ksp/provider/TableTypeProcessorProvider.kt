package org.snappy.mssql.ksp.provider

import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import org.snappy.mssql.ksp.processor.TableTypeProcessor

class TableTypeProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor {
        return TableTypeProcessor(
            environment.codeGenerator,
            environment.logger,
        )
    }
}