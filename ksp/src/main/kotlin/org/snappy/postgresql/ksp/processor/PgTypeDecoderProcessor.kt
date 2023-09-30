package org.snappy.postgresql.ksp.processor

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.KSVisitorVoid
import com.google.devtools.ksp.validate
import org.snappy.ksp.appendText
import org.snappy.ksp.hasAnnotation
import org.snappy.postgresql.type.PgType

class PgTypeDecoderProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
) : SymbolProcessor {
    override fun process(resolver: Resolver): List<KSAnnotated> {
        val symbols = resolver.getSymbolsWithAnnotation(PgType::class.java.name)
        val result = symbols.filter { !it.validate() }.toList()
        symbols.filter {
            it is KSClassDeclaration && it.validate()
        }.forEach {
            it.accept(CompositeVisitor(), Unit)
        }
        return result
    }

    inner class CompositeVisitor : KSVisitorVoid() {
        private val imports = mutableSetOf(
            "org.snappy.postgresql.type.PgObjectDecoder",
            "org.postgresql.util.PGobject",
        )

        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            classDeclaration.primaryConstructor!!.accept(this, data)
        }

        private fun addImport(import: String) {
            if (import.startsWith("kotlin.")) return
            imports += import
        }

        private val importsSorted: List<String> get() = imports.sorted()

        private fun createVariableDeclarationFromParameter(
            parameter: KSValueParameter,
        ): String {
            val name = parameter.name?.asString()
                ?: error("Could not find name for ${parameter.name?.asString()}")
            val parameterType = parameter.type.resolve()
            val isParameterNullable = parameterType.isMarkedNullable
            val parameterTypeDeclaration = parameterType.declaration
            val packageName = parameterTypeDeclaration.packageName.asString()
            val simpleName = parameterTypeDeclaration.simpleName.asString()

            val readType = when {
                parameterTypeDeclaration.hasAnnotation<PgType>() -> {
                    addImport("$packageName.$simpleName")
                    "Composite<$simpleName>"
                }
                simpleName == "Array" || simpleName == "List" -> {
                    val collectionType = parameterType.arguments
                        .first()
                        .type!!
                        .resolve()
                        .declaration
                    val collectionTypeSimpleName = collectionType.simpleName.asString()
                    val collectionTypePackage = collectionType.packageName.asString()
                    addImport("$collectionTypePackage.$collectionTypeSimpleName")
                    "$simpleName<$collectionTypeSimpleName>"
                }
                simpleName in validReadTypes -> simpleName
                else -> error("Field $name is not able to be parsed from a composite literal")
            }
            val nullCheck = if (!isParameterNullable) {
                " ?: error(\"Parameter '$name' cannot be null\")"
            } else ""
            return "val $name = read$readType()${nullCheck}"
        }

        private fun getCompositeLiteralParser(
            declaration: KSClassDeclaration,
            constructor: KSFunctionDeclaration,
        ): String {
            val params = constructor.parameters
            val variables = params.joinToString(
                separator = "\n                            ",
                postfix = "\n                            ",
            ) {
                createVariableDeclarationFromParameter(it)
            }
            val returnExpression = params.joinToString(
                separator = ",\n                                ",
                prefix = "${declaration.simpleName.asString()}(\n                                ",
                postfix = ",\n                            )",
            ) {
                it.name?.asString() ?: error("Could not find name for $it")
            }
            return "$variables$returnExpression"
        }

        private fun createCompositeDecoderFile(
            constructorFunction: KSFunctionDeclaration,
            classDeclaration: KSClassDeclaration,
        ) {
            val classPackage = classDeclaration.packageName.asString()
            val className = classDeclaration.simpleName.asString()
            val decoderName = "${className}Decoder"
            val file = codeGenerator.createNewFile(
                dependencies = Dependencies(true, constructorFunction.containingFile!!),
                packageName = DESTINATION_PACKAGE,
                fileName = decoderName,
            )

            addImport("$classPackage.$className")
            addImport("org.snappy.postgresql.literal.parseComposite")
            val parserBody = getCompositeLiteralParser(classDeclaration, constructorFunction)
            val importsOrdered = importsSorted.joinToString(
                separator = "\n                ",
            ) {
                "import $it"
            }
            file.appendText("""
                @file:Suppress("UNUSED")
                package $DESTINATION_PACKAGE
                
                $importsOrdered
                
                class $decoderName : PgObjectDecoder<$className> {
                    override fun decodePgObject(pgObject: PGobject): $className? {
                        return parseComposite(pgObject) {
                            $parserBody
                        }
                    }
                }
                
            """.trimIndent())
        }

        private fun createEnumDecoderFile(
            constructorFunction: KSFunctionDeclaration,
            classDeclaration: KSClassDeclaration,
        ) {
            val classPackage = classDeclaration.packageName.asString()
            val className = classDeclaration.simpleName.asString()
            val decoderName = "${className}Decoder"
            val file = codeGenerator.createNewFile(
                dependencies = Dependencies(true, constructorFunction.containingFile!!),
                packageName = DESTINATION_PACKAGE,
                fileName = decoderName,
            )

            addImport("$classPackage.$className")
            addImport("org.snappy.decodeError")
            val importsOrdered = importsSorted.joinToString(
                separator = "\n                ",
            ) {
                "import $it"
            }
            file.appendText("""
                @file:Suppress("UNUSED")
                package $DESTINATION_PACKAGE
                
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

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            val classDeclaration = function.parentDeclaration as? KSClassDeclaration ?: return
            if (classDeclaration.typeParameters.any()) {
                error("Generic PgType classes are not supported")
            }
            if (classDeclaration.classKind == ClassKind.ENUM_CLASS) {
                createEnumDecoderFile(function, classDeclaration)
                logger.info("Created decoder file for ${classDeclaration.simpleName.asString()}")
                return
            }
            createCompositeDecoderFile(function, classDeclaration)
            logger.info("Created decoder file for ${classDeclaration.simpleName.asString()}")
        }
    }

    companion object {
        const val DESTINATION_PACKAGE: String = "org.snappy.postgresql.composite.decoders"
        val validReadTypes = listOf(
            "Boolean",
            "Short",
            "Int",
            "Long",
            "Float",
            "Double",
            "BigDecimal",
            "String",
            "LocalDate",
            "LocalTime",
            "LocalDateTime",
            "OffsetTime",
            "OffsetDateTime",
            "Instant",
        )
    }
}
