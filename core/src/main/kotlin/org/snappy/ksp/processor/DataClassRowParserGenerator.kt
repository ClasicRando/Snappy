package org.snappy.ksp.processor

import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSValueParameter
import org.snappy.ksp.AbstractFileGenerator
import org.snappy.ksp.appendText

class DataClassRowParserGenerator(
    private val classDeclaration: KSClassDeclaration,
    private val constructor: KSFunctionDeclaration,
) : AbstractFileGenerator() {
    init {
        addImport("org.snappy.rowparse.SnappyRow")
        addImport("org.snappy.SnappyMapper")
        addImport("org.snappy.rowparse.RowParser")
    }

    inner class ParameterDetails(
        parameter: KSValueParameter,
        propertyAnnotations: Map<String, PropertyAnnotations>,
    ) {
        private val paramName = parameter.name!!.asString()
        private val annotations = propertyAnnotations[paramName]
        private val fieldName = annotations?.rename ?: paramName
        private val paramType = parameter.type.resolve()
        private val paramDeclaration = paramType.declaration as KSClassDeclaration
        private val paramDeclarationPackage = paramDeclaration.packageName.asString()
        private val paramDeclarationName = paramDeclaration.simpleName.asString()
        private val genericType = if (paramDeclaration.typeParameters.isNotEmpty()) {
            if (paramDeclarationName != "List" && paramDeclarationName != "Array") {
                error("Generic types in row parser fields are not supported outside of List and Array")
            }
            val collectionType = paramType.arguments
                .first()
                .type!!
                .resolve()
                .declaration
            val collectionSimpleName = collectionType.simpleName.asString()
            addImport("${collectionType.packageName.asString()}.$collectionSimpleName")
            "<$collectionSimpleName>"
        } else ""
        private val parserPropertyName = if (annotations?.flatten == true) {
            "${paramName}RowParser"
        } else {
            "${paramName}Decoder"
        }
        val parserProperty = if (annotations?.flatten == true) {
            """
                private val $parserPropertyName = SnappyMapper.rowParserCache.getOrNull<$paramDeclarationName$genericType>()
                    ?: error("Could not find a row parser for type '$paramDeclarationName$genericType'")
            """.trimIndent()
        }
        else {
            """
                private val $parserPropertyName = SnappyMapper.decoderCache.getOrNull<$paramDeclarationName$genericType>()
                    ?: error("Could not find a decoder for type '$paramDeclarationName$genericType'")
            """.trimIndent()
        }
        fun constructorParameter(): String {
            val decodeMethod = when {
                annotations?.flatten == true -> "parseRow"
                paramType.isMarkedNullable -> "decodeNullable"
                else -> "decode"
            }
            val extraParameters = if (annotations?.flatten == true) "" else ", \"$fieldName\""
            return "$paramName = $parserPropertyName.$decodeMethod(row$extraParameters)"
        }

        init {
            addImport("$paramDeclarationPackage.$paramDeclarationName")
        }
    }

    override fun generateFile(codeGenerator: CodeGenerator) {
        val classPackage = classDeclaration.packageName.asString()
        val className = classDeclaration.simpleName.asString()
        val rowParserName = "${className}RowParser"
        val file = codeGenerator.createNewFile(
            dependencies = Dependencies(true, constructor.containingFile!!),
            packageName = RowParserProcessor.DESTINATION_PACKAGE,
            fileName = rowParserName,
        )
        addImport("$classPackage.$className")
        val propertyAnnotations = classDeclaration.getDeclaredProperties()
            .map { prop ->
                val annotations = prop.annotations
                    .fold(PropertyAnnotations.default) { acc, annotation ->
                        acc.merge(RowParserProcessor.mapPropertyAnnotations(annotation))
                    }
                prop.simpleName.asString() to annotations
            }
            .filter { (_, annotations) -> annotations.hasValues }
            .toMap()
        val parameters = constructor.parameters.map { parameter ->
            ParameterDetails(parameter, propertyAnnotations)
        }
        val decoderProperties = parameters.joinToString(
            separator = "\n",
            transform = ParameterDetails::parserProperty,
        ).replaceIndent("                    ").trim()
        val constructorCall = parameters.joinToString(
            prefix = "$className(\n                            ",
            separator = ",\n                            ",
            postfix = ",\n                        )",
            transform = ParameterDetails::constructorParameter,
        )

        val importsOrdered = importsSorted.joinToString(
            separator = "\n                ",
        ) {
            "import $it"
        }
        file.appendText("""
            @file:Suppress("UNUSED")
            package ${RowParserProcessor.DESTINATION_PACKAGE}
            
            $importsOrdered
            
            class $rowParserName : RowParser<$className> {
                $decoderProperties
            
                override fun parseRow(row: SnappyRow): $className {
                    return $constructorCall
                }
            }
            
        """.trimIndent())
    }
}