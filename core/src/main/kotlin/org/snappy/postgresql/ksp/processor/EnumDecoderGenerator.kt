package org.snappy.postgresql.ksp.processor

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import org.snappy.ksp.AbstractFileGenerator
import org.snappy.ksp.appendText

class EnumDecoderGenerator(
    private val constructorFunction: KSFunctionDeclaration,
    private val classDeclaration: KSClassDeclaration,
    private val logger: KSPLogger,
) : AbstractFileGenerator() {
    init {
        addImport("org.snappy.postgresql.type.PgObjectDecoder")
        addImport("org.postgresql.util.PGobject")
        addImport("org.snappy.decodeError")
    }

    override fun generateFile(codeGenerator: CodeGenerator) {
        val classPackage = classDeclaration.packageName.asString()
        val className = classDeclaration.simpleName.asString()
        val decoderName = "${className}Decoder"
        val file = codeGenerator.createNewFile(
            dependencies = Dependencies(true, constructorFunction.containingFile!!),
            packageName = PgTypeDecoderProcessor.DESTINATION_PACKAGE,
            fileName = decoderName,
        )

        addImport("$classPackage.$className")
        val importsOrdered = importsSorted.joinToString(
            separator = "\n                ",
        ) {
            "import $it"
        }
        file.appendText("""
            @file:Suppress("UNUSED")
            package ${PgTypeDecoderProcessor.DESTINATION_PACKAGE}
            
            $importsOrdered
            
            class $decoderName : PgObjectDecoder<$className> {
                override fun decodePgObject(pgObject: PGobject): $className? {
                    return pgObject.value?.let { value ->
                        enumValues<$className>().find {
                            value.lowercase() == it.name.lowercase()
                        } ?: decodeError<$className>(pgObject.value)
                    }
                }
            }
            
        """.trimIndent())
    }
}