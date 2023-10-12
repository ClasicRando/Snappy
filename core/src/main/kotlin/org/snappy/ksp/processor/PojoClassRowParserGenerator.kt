package org.snappy.ksp.processor

import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import org.snappy.ksp.AbstractFileGenerator
import org.snappy.ksp.appendText

class PojoClassRowParserGenerator(
    private val classDeclaration: KSClassDeclaration,
) : AbstractFileGenerator() {
    init {
        addImport("org.snappy.rowparse.SnappyRow")
        addImport("org.snappy.SnappyMapper")
        addImport("org.snappy.rowparse.RowParser")
    }

    inner class PropertyDetails(
        property: KSPropertyDeclaration,
        propertyAnnotations: Map<String, PropertyAnnotations>,
    ) {
        private val propertyName = property.simpleName.asString()
        private val annotations = propertyAnnotations[propertyName]
        private val fieldName = annotations?.rename ?: propertyName
        private val propertyType = property.type.resolve()
        private val propertyTypeDeclaration = propertyType.declaration as KSClassDeclaration
        private val propertyTypeDeclarationPackage = propertyTypeDeclaration.packageName.asString()
        private val propertyTypeDeclarationName = propertyTypeDeclaration.simpleName.asString()
        private val genericType = if (propertyTypeDeclaration.typeParameters.isNotEmpty()) {
            if (propertyTypeDeclarationName != "List" && propertyTypeDeclarationName != "Array") {
                error("Generic types in row parser fields are not supported outside of List and Array")
            }
            val collectionType = propertyType.arguments
                .first()
                .type!!
                .resolve()
                .declaration
            val collectionSimpleName = collectionType.simpleName.asString()
            addImport("${collectionType.packageName.asString()}.$collectionSimpleName")
            "<$collectionSimpleName>"
        } else ""
        private val parserPropertyName = if (annotations?.flatten == true) {
            "${propertyName}RowParser"
        } else {
            "${propertyName}Decoder"
        }
        val parserProperty = if (annotations?.flatten == true) {
            """
                private val $parserPropertyName = SnappyMapper.rowParserCache.getOrNull<$propertyTypeDeclarationName$genericType>()
                    ?: error("Could not find a row parser for type '$propertyTypeDeclarationName$genericType'")
            """.trimIndent()
        }
        else {
            """
                private val $parserPropertyName = SnappyMapper.decoderCache.getOrNull<$propertyTypeDeclarationName$genericType>()
                    ?: error("Could not find a decoder for type '$propertyTypeDeclarationName$genericType'")
            """.trimIndent()
        }
        fun propertySetAction(): String {
            val decodeMethod = when {
                annotations?.flatten == true -> "parseRow"
                propertyType.isMarkedNullable -> "decodeNullable"
                else -> "decode"
            }
            val extraParameters = if (annotations?.flatten == true) "" else ", \"$fieldName\""
            return "if (row.containsKey(\"$fieldName\")) { $propertyName = $parserPropertyName.$decodeMethod(row$extraParameters) }"
        }

        init {
            addImport("$propertyTypeDeclarationPackage.$propertyTypeDeclarationName")
        }
    }

    override fun generateFile(codeGenerator: CodeGenerator) {
        val classPackage = classDeclaration.packageName.asString()
        val className = classDeclaration.simpleName.asString()
        val rowParserName = "${className}RowParser"
        val file = codeGenerator.createNewFile(
            dependencies = Dependencies(true, classDeclaration.containingFile!!),
            packageName = RowParserProcessor.DESTINATION_PACKAGE,
            fileName = rowParserName,
        )
        addImport("$classPackage.$className")
        val setProperties = classDeclaration.getDeclaredProperties()
            .filter { it.setter != null }
        val propertyAnnotations = setProperties
            .map { prop ->
                val annotations = prop.annotations
                    .fold(PropertyAnnotations.default) { acc, annotation ->
                        acc.merge(RowParserProcessor.mapPropertyAnnotations(annotation))
                    }
                prop.simpleName.asString() to annotations
            }
            .filter { (_, annotations) -> annotations.hasValues }
            .toMap()

        val properties = setProperties.map {
            PropertyDetails(it, propertyAnnotations)
        }
        val decoderProperties = properties.joinToString(
            separator = "\n",
            transform = PropertyDetails::parserProperty,
        ).replaceIndent("                    ").trim()
        val objectCreation = properties.joinToString(
            prefix = "$className().apply {\n                            ",
            separator = "\n                            ",
            postfix = "\n                        }",
            transform = PropertyDetails::propertySetAction,
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
                    return $objectCreation
                }
            }
            
        """.trimIndent())
    }
}