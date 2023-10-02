package org.snappy.ksp.processor

import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.KSVisitorVoid
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.validate
import org.snappy.ksp.appendText
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

    private val columnKClass = Rename::class

    inner class RowParserVisitor : KSVisitorVoid() {
        inner class ParameterDetails(
            parameter: KSValueParameter,
            propertyAnnotations: Map<String, String>,
        ) {
            private val paramName = parameter.name!!.asString()
            private val fieldName = propertyAnnotations[paramName] ?: paramName
            private val paramType = parameter.type.resolve()
            private val paramDeclaration = paramType.declaration as KSClassDeclaration
            private val paramDeclarationPackage = paramDeclaration.packageName.asString()
            private val paramDeclarationName = paramDeclaration.simpleName.asString()
            val decoderProperty = """
                private val ${paramName}Decoder = SnappyMapper.decoderCache.getOrNull<$paramDeclarationName>()
                    ?: error("Could not find a decoder for type '$paramDeclarationName'")
            """.trimIndent()
            fun constructorParameter(): String {
                val decodeMethod = if (paramType.isMarkedNullable) {
                    "decodeNullable"
                } else {
                    "decode"
                }
                return "$paramName = ${paramName}Decoder.$decodeMethod(row, \"$fieldName\")"
            }

            init {
                addImport("$paramDeclarationPackage.$paramDeclarationName")
            }
        }

        inner class PropertyDetails(
            property: KSPropertyDeclaration,
            propertyAnnotations: Map<String, String>,
        ) {
            private val propertyName = property.simpleName.asString()
            private val fieldName = propertyAnnotations[propertyName] ?: propertyName
            private val propertyType = property.type.resolve()
            private val propertyTypeDeclaration = propertyType.declaration as KSClassDeclaration
            private val propertyTypeDeclarationPackage = propertyTypeDeclaration.packageName.asString()
            private val propertyTypeDeclarationName = propertyTypeDeclaration.simpleName.asString()
            val decoderProperty = """
                private val ${propertyName}Decoder = SnappyMapper.decoderCache.getOrNull<$propertyTypeDeclarationName>()
                    ?: error("Could not find a decoder for type '$propertyTypeDeclarationName'")
            """.trimIndent()
            fun propertySetAction(): String {
                val decodeMethod = if (propertyType.isMarkedNullable) {
                    "decodeNullable"
                } else {
                    "decode"
                }
                return "if (row.containsKey(\"$fieldName\")) { $propertyName = ${propertyName}Decoder.$decodeMethod(row, \"$fieldName\") }"
            }

            init {
                addImport("$propertyTypeDeclarationPackage.$propertyTypeDeclarationName")
            }
        }

        private val imports = mutableSetOf(
            "org.snappy.rowparse.SnappyRow",
            "org.snappy.SnappyMapper",
            "org.snappy.rowparse.RowParser",
        )

        private fun addImport(import: String) {
            if (import.startsWith("kotlin.")) return
            imports += import
        }

        private val importsSorted: List<String> get() = imports.sorted()

        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            classDeclaration.primaryConstructor!!.accept(this, data)
        }

        private fun generatePojoClassRowParser(classDeclaration: KSClassDeclaration) {
            val classPackage = classDeclaration.packageName.asString()
            val className = classDeclaration.simpleName.asString()
            val rowParserName = "${className}RowParser"
            val file = codeGenerator.createNewFile(
                dependencies = Dependencies(true, classDeclaration.containingFile!!),
                packageName = DESTINATION_PACKAGE,
                fileName = rowParserName,
            )
            addImport("$classPackage.$className")
            val setProperties = classDeclaration.getDeclaredProperties()
                .filter { it.setter != null }
            val propertyAnnotations = setProperties
                .mapNotNull { prop ->
                    val name = prop.annotations.filter {
                        val annotationDeclaration = it.annotationType.resolve().declaration
                        it.shortName.getShortName() == columnKClass.simpleName
                                && annotationDeclaration.qualifiedName?.asString() == columnKClass.qualifiedName
                    }.map {
                        it.arguments[0].value as String
                    }.firstOrNull() ?: return@mapNotNull null
                    prop.simpleName.asString() to name
                }
                .toMap()

            val properties = setProperties.map {
                PropertyDetails(it, propertyAnnotations)
            }
            val decoderProperties = properties.joinToString(
                separator = "\n",
                transform = PropertyDetails::decoderProperty,
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
                package $DESTINATION_PACKAGE
                
                $importsOrdered
                
                class $rowParserName : RowParser<$className> {
                    $decoderProperties
                
                    override fun parseRow(row: SnappyRow): $className {
                        return $objectCreation
                    }
                }
                
            """.trimIndent())
        }

        private fun generateDataClassRowParser(
            constructor: KSFunctionDeclaration,
            classDeclaration: KSClassDeclaration,
        ) {
            val classPackage = classDeclaration.packageName.asString()
            val className = classDeclaration.simpleName.asString()
            val rowParserName = "${className}RowParser"
            val file = codeGenerator.createNewFile(
                dependencies = Dependencies(true, constructor.containingFile!!),
                packageName = DESTINATION_PACKAGE,
                fileName = rowParserName,
            )
            addImport("$classPackage.$className")
            val propertyAnnotations = classDeclaration.getDeclaredProperties()
                .mapNotNull { prop ->
                    val name = prop.annotations.filter {
                        val annotationDeclaration = it.annotationType.resolve().declaration
                        it.shortName.getShortName() == columnKClass.simpleName
                                && annotationDeclaration.qualifiedName?.asString() == columnKClass.qualifiedName
                    }.map {
                        it.arguments[0].value as String
                    }.firstOrNull() ?: return@mapNotNull null
                    prop.simpleName.asString() to name
                }
                .toMap()
            val parameters = constructor.parameters.map { parameter ->
                ParameterDetails(parameter, propertyAnnotations)
            }
            val decoderProperties = parameters.joinToString(
                separator = "\n",
                transform = ParameterDetails::decoderProperty,
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
                package $DESTINATION_PACKAGE
                
                $importsOrdered
                
                class $rowParserName : RowParser<$className> {
                    $decoderProperties
                
                    override fun parseRow(row: SnappyRow): $className {
                        return $constructorCall
                    }
                }
                
            """.trimIndent())
        }

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            val classDeclaration = function.parentDeclaration as? KSClassDeclaration ?: return
            val simpleName = classDeclaration.simpleName.asString()
            if (classDeclaration.typeParameters.any()) {
                error("Cannot generate row parser for class with generic parameters, '$simpleName'")
            }
            if (classDeclaration.modifiers.contains(Modifier.DATA)) {
                generateDataClassRowParser(function, classDeclaration)
                logger.info("Created data class row parser for '$simpleName'")
                return
            }
            if (function.parameters.any()) {
                error("Cannot generate Pojo row parser if constructor has parameters, '$simpleName'")
            }
            generatePojoClassRowParser(classDeclaration)
            logger.info("Created pojo class row parser for '$simpleName'")
        }
    }

    companion object {
        const val DESTINATION_PACKAGE: String = "org.snappy.rowparse.parsers"
    }
}