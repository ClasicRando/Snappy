package org.snappy.ksp.processor

import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import com.google.devtools.ksp.symbol.KSType
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.validate
import org.snappy.ksp.FileGeneratorVisitor
import org.snappy.ksp.appendText
import org.snappy.ksp.symbols.Flatten
import org.snappy.ksp.symbols.Rename
import org.snappy.ksp.symbols.RowParser

class RowParserProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
) : SymbolProcessor {
    override fun process(resolver: Resolver): List<KSAnnotated> {
        val symbols = resolver.getSymbolsWithAnnotation(RowParser::class.java.name)
        val result = symbols.filter { !it.validate() }.toList()
        symbols.filter {
            it is KSClassDeclaration && it.validate()
        }.forEach {
            it.accept(RowParserVisitor(), Unit)
        }
        return result
    }

    data class GeneratedPair(val parserProperty: String, val objectCreationParameter: String)

    inner class RowParserVisitor : FileGeneratorVisitor() {
        lateinit var simpleName: String
        private lateinit var classPackage: String
        private lateinit var classDeclaration: KSClassDeclaration
        private var isDataClass = false
        private val propertyAnnotations = mutableMapOf<String, PropertyAnnotations>()
        private val generatedPairs = mutableListOf<GeneratedPair>()

        private fun addGeneratedPair(name: String, type: KSType, isPropertySet: Boolean) {
            val annotations = propertyAnnotations[name]
            val fieldName = annotations?.rename ?: name
            val isFlatten = annotations?.flatten == true
            val paramDeclaration = type.declaration as KSClassDeclaration
            val paramDeclarationPackage = paramDeclaration.packageName.asString()
            val paramDeclarationName = paramDeclaration.simpleName.asString()
            val genericType = if (paramDeclaration.typeParameters.isNotEmpty()) {
                if (paramDeclarationName != "List" && paramDeclarationName != "Array") {
                    error("Generic types in row parser fields are not supported outside of List and Array")
                }
                val collectionType = type.arguments
                    .first()
                    .type!!
                    .resolve()
                    .declaration
                val collectionSimpleName = collectionType.simpleName.asString()
                addImport("${collectionType.packageName.asString()}.$collectionSimpleName")
                "<$collectionSimpleName>"
            } else ""
            val parserPropertyName = if (isFlatten) {
                "${name}RowParser"
            } else {
                "${name}Decoder"
            }
            val parserProperty = if (isFlatten) {
                """
                    private val $parserPropertyName by lazy {
                        SnappyMapper.rowParserCache.getOrNull<$paramDeclarationName$genericType>()
                            ?: error("Could not find a row parser for type '$paramDeclarationName$genericType'")
                    }
                """.trimIndent()
            }
            else {
                """
                    private val $parserPropertyName by lazy {
                        SnappyMapper.decoderCache.getOrNull<$paramDeclarationName$genericType>()
                            ?: error("Could not find a decoder for type '$paramDeclarationName$genericType'")
                    }
                """.trimIndent()
            }
            val decodeMethod = when {
                isFlatten -> "parseRow"
                type.isMarkedNullable -> "decodeNullable"
                else -> "decode"
            }
            val extraParameters = if (isFlatten) "" else ", \"$fieldName\""
            val constructorParameter = if (isPropertySet) {
                "if (row.containsKey(\"$fieldName\")) { $name = $parserPropertyName.$decodeMethod(row$extraParameters) }"
            } else {
                "$name = $parserPropertyName.$decodeMethod(row$extraParameters)"
            }

            addImport("$paramDeclarationPackage.$paramDeclarationName")
            generatedPairs += GeneratedPair(parserProperty, constructorParameter)
        }

        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            this.classDeclaration = classDeclaration
            simpleName = classDeclaration.simpleName.asString()
            classPackage = classDeclaration.packageName.asString()
            isDataClass = classDeclaration.modifiers.contains(Modifier.DATA)
            if (classDeclaration.typeParameters.any()) {
                error("Cannot generate row parser for class with generic parameters, '$simpleName'")
            }
            classDeclaration.getDeclaredProperties().forEach {
                it.accept(this, data)
            }
            addImport("$classPackage.$simpleName")
            addImport("org.snappy.rowparse.SnappyRow")
            addImport("org.snappy.SnappyMapper")
            addImport("org.snappy.rowparse.RowParser")
            if (isDataClass) {
                classDeclaration.primaryConstructor!!.accept(this, data)
            } else if (classDeclaration.primaryConstructor!!.parameters.any()) {
                error("Cannot generate Pojo row parser if constructor has parameters, '$simpleName'")
            }
            generateFile(codeGenerator)
            logger.info(
                "Created ${if (isDataClass) "data" else "pojo"} class row parser for '$simpleName'"
            )
        }

        override fun visitValueParameter(valueParameter: KSValueParameter, data: Unit) {
            addGeneratedPair(
                name = valueParameter.name!!.asString(),
                type = valueParameter.type.resolve(),
                isPropertySet = false,
            )
        }

        override fun visitPropertyDeclaration(property: KSPropertyDeclaration, data: Unit) {
            val annotationData = property.annotations
                .fold(PropertyAnnotations.default) { acc, annotation ->
                    acc.merge(mapPropertyAnnotations(annotation))
                }
            if (annotationData.hasValues) {
                propertyAnnotations[property.simpleName.asString()] = annotationData
            }

            if (isDataClass) {
                return
            }
            addGeneratedPair(
                name = property.simpleName.asString(),
                type = property.type.resolve(),
                isPropertySet = true,
            )
        }

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            for (parameter in function.parameters) {
                parameter.accept(this, data)
            }
        }

        override fun generateFile(codeGenerator: CodeGenerator) {
            val rowParserName = "${simpleName}RowParser"
            val file = codeGenerator.createNewFile(
                dependencies = Dependencies(true, classDeclaration.containingFile!!),
                packageName = DESTINATION_PACKAGE,
                fileName = rowParserName,
            )

            val decoderProperties = generatedPairs.joinToString(
                separator = "\n",
                transform = GeneratedPair::parserProperty,
            ).replaceIndent("                    ").trim()
            val constructorCall = generatedPairs.joinToString(
                prefix = if (isDataClass) {
                    "$simpleName(\n                            "
                } else {
                    "$simpleName().apply {\n                            "
                },
                separator = if (isDataClass) {
                    ",\n                            "
                } else {
                    "\n                            "
                },
                postfix = if (isDataClass) {
                    ",\n                        )"
                } else {
                    "\n                        }"
                },
                transform = GeneratedPair::objectCreationParameter,
            )

            val importsOrdered = importsSorted.joinToString(
                separator = "\n                ",
            ) {
                "import $it"
            }
            file.appendText("""
                @file:Suppress("UNUSED")
                package $DESTINATION_PACKAGE
                
                $importsOrdered
                
                class $rowParserName : RowParser<$simpleName> {
                    $decoderProperties
                
                    override fun parseRow(row: SnappyRow): $simpleName {
                        return $constructorCall
                    }
                }
                
            """.trimIndent())
        }
    }

    companion object {
        private val renameKClass = Rename::class
        private val flattenKClass = Flatten::class
        const val DESTINATION_PACKAGE: String = "org.snappy.rowparse.parsers"

        fun mapPropertyAnnotations(annotation: KSAnnotation): PropertyAnnotations {
            val annotationDeclaration = annotation.annotationType.resolve().declaration
            val shortName = annotation.shortName.getShortName()
            val qualifiedName = annotationDeclaration.qualifiedName?.asString()
            if (shortName == renameKClass.simpleName && qualifiedName == renameKClass.qualifiedName) {
                return PropertyAnnotations(rename = annotation.arguments[0].value as String)
            }
            if (shortName == flattenKClass.simpleName && qualifiedName == flattenKClass.qualifiedName) {
                return PropertyAnnotations(flatten = true)
            }
            return PropertyAnnotations.default
        }
    }
}