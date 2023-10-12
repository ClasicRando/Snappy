package org.snappy.postgresql.ksp.processor

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import org.snappy.ksp.AbstractFileGenerator
import org.snappy.ksp.appendText

class ArrayDecoderGenerator(
    private val constructorFunction: KSFunctionDeclaration,
    private val classDeclaration: KSClassDeclaration,
    private val logger: KSPLogger,
) : AbstractFileGenerator() {
    init {
        addImport("org.postgresql.util.PGobject")
        addImport("org.snappy.decodeError")
        addImport("org.snappy.decode.Decoder")
        addImport("org.snappy.rowparse.SnappyRow")
    }

    override fun generateFile(codeGenerator: CodeGenerator) {
        val classPackage = classDeclaration.packageName.asString()
        val className = classDeclaration.simpleName.asString()
        val file = codeGenerator.createNewFile(
            dependencies = Dependencies(true, constructorFunction.containingFile!!),
            packageName = PgTypeDecoderProcessor.DESTINATION_PACKAGE,
            fileName = "${className}ArrayDecoders",
        )

        addImport("$classPackage.$className")
        val importsOrdered = importsSorted.joinToString(
            separator = "\n            ",
        ) {
            "import $it"
        }
        file.appendText("""
            @file:Suppress("UNUSED")
            package ${PgTypeDecoderProcessor.DESTINATION_PACKAGE}
            
            $importsOrdered
            
            class ${className}ListDecoder: Decoder<List<$className>> {
                private val decoder = ${className}Decoder()
            
                override fun decodeNullable(row: SnappyRow, fieldName: String): List<$className>? {
                    val sqlArray = row.getArray(fieldName)
                    if (sqlArray.array == null) {
                        return null
                    }
                    val array = sqlArray.array as Array<*>
                    return array.map {
                        checkNotNull(it) { "Array Element cannot be null" }
                        val obj = it as? PGobject ?: decodeError<PGobject>(it)
                        decoder.decodePgObject(obj) ?: error("Array Element cannot be null")
                    }
                }
            }
            
            class ${className}ListNullableDecoder: Decoder<List<$className?>> {
                private val decoder = ${className}Decoder()
                    
                override fun decodeNullable(row: SnappyRow, fieldName: String): List<$className?>? {
                    val sqlArray = row.getArray(fieldName)
                    if (sqlArray.array == null) {
                        return null
                    }
                    val array = sqlArray.array as Array<*>
                    return array.map {
                        if (it == null) return@map null
                        val obj = it as? PGobject ?: decodeError<PGobject>(it)
                        decoder.decodePgObject(obj)
                    }
                }
            }
            
        """.trimIndent())
    }
}