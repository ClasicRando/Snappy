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
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.validate
import org.snappy.ksp.FileGeneratorVisitor
import org.snappy.ksp.appendText
import org.snappy.ksp.hasAnnotation
import org.snappy.ksp.isInstance
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

    data class CompositeLiteralParserPair(
        val variableDeclaration: String,
        val constructorParameter: String,
    )

    inner class CompositeVisitor : FileGeneratorVisitor() {
        private lateinit var classDeclaration: KSClassDeclaration
        private lateinit var classPackage: String
        private lateinit var className: String
        private var isEnum = false
        private var createArrayDecoder = false
        private val parserPairList = mutableListOf<CompositeLiteralParserPair>()

        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            if (classDeclaration.typeParameters.any()) {
                error("Generic PgType classes are not supported")
            }
            isEnum = classDeclaration.classKind == ClassKind.ENUM_CLASS
            if (!classDeclaration.modifiers.contains(Modifier.DATA) && !isEnum) {
                error("Only data classes can be used as postgresql type")
            }

            this.classDeclaration = classDeclaration
            classPackage = classDeclaration.packageName.asString()
            className = classDeclaration.simpleName.asString()
            val annotationArguments = classDeclaration.annotations
                .first { it.isInstance<PgType>() }
                .arguments
            createArrayDecoder = annotationArguments[1].value as Boolean
            val createDecoder = annotationArguments[2].value as Boolean
            if (!createDecoder) {
                return
            }

            if (isEnum) {
                return
            } else {
                addImport("org.snappy.postgresql.type.PgObjectDecoder")
                addImport("org.postgresql.util.PGobject")
                addImport("org.snappy.postgresql.literal.parseComposite")
                addImport("$classPackage.$className")
                classDeclaration.primaryConstructor!!.accept(this, data)
            }
            generateFile(codeGenerator)
            if (createArrayDecoder) {
                ArrayDecoderGenerator(classDeclaration, logger).generateFile(codeGenerator)
            }
        }

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            for (parameter in function.parameters) {
                parameter.accept(this, data)
            }
        }

        override fun visitValueParameter(valueParameter: KSValueParameter, data: Unit) {
            val name = valueParameter.name?.asString()
                ?: error("Could not find name for ${valueParameter.name?.asString()}")
            val parameterType = valueParameter.type.resolve()
            val isParameterNullable = parameterType.isMarkedNullable
            val parameterTypeDeclaration = parameterType.declaration as KSClassDeclaration
            val packageName = parameterTypeDeclaration.packageName.asString()
            val simpleName = parameterTypeDeclaration.simpleName.asString()

            val nullCheck = if (!isParameterNullable) {
                " ?: error(\"Parameter '$name' cannot be null\")"
            } else ""
            val readType = when {
                parameterTypeDeclaration.hasAnnotation<PgType>() || simpleName == "PgJson" -> {
                    addImport("$packageName.$simpleName")
                    "Composite<$simpleName>()"
                }
                parameterTypeDeclaration.classKind == ClassKind.ENUM_CLASS -> {
                    addImport("$packageName.$simpleName")
                    "Enum<$simpleName>()"
                }
                parameterTypeDeclaration.modifiers.any { it == Modifier.VALUE } -> {
                    val valueTypeParameter = parameterTypeDeclaration.primaryConstructor!!
                        .parameters
                        .first()
                    val valueParameterTypeDeclaration = valueTypeParameter.type
                        .resolve().declaration as KSClassDeclaration
                    val parameterTypeName = valueParameterTypeDeclaration.simpleName.asString()
                    if (parameterTypeName !in PgTypeEncoderProcessor.validAppendTypes) {
                        error("Value class must have valid append type")
                    }
                    addImport("$packageName.$simpleName")
                    val pair = CompositeLiteralParserPair(
                        variableDeclaration = "val $name = $simpleName(read$parameterTypeName()$nullCheck)",
                        constructorParameter = name
                    )
                    parserPairList += pair
                    return
                }
                simpleName == "Array" || simpleName == "List" -> {
                    val collectionType = parameterType.arguments
                        .first()
                        .type!!
                        .resolve()
                    val mapNotNull = if (!collectionType.isMarkedNullable) {
                        "?.map { checkNotNull(it); it }"
                    } else ""
                    val collectionTypeDeclaration = collectionType.declaration
                    val collectionTypeSimpleName = collectionTypeDeclaration.simpleName.asString()
                    val collectionTypePackage = collectionTypeDeclaration.packageName.asString()
                    addImport("$collectionTypePackage.$collectionTypeSimpleName")
                    "$simpleName<$collectionTypeSimpleName>()$mapNotNull"
                }
                simpleName in validReadTypes -> "$simpleName()"
                else -> error("Field $name is not able to be parsed from a composite literal")
            }
            val pair = CompositeLiteralParserPair(
                variableDeclaration = "val $name = read$readType$nullCheck",
                constructorParameter = name
            )
            parserPairList += pair
        }

        override fun generateFile(codeGenerator: CodeGenerator) {
            val decoderName = "${className}Decoder"
            val file = codeGenerator.createNewFile(
                dependencies = Dependencies(true, classDeclaration.containingFile!!),
                packageName = DESTINATION_PACKAGE,
                fileName = decoderName,
            )

            addImport("$classPackage.$className")
            val importsOrdered = importsSorted.joinToString(
                separator = "\n                    ",
            ) {
                "import $it"
            }
            if (isEnum) {
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
            } else {
                val variables = parserPairList.joinToString(
                    separator = "\n                                ",
                    postfix = "\n                                ",
                    transform = CompositeLiteralParserPair::variableDeclaration,
                )
                val returnExpression = parserPairList.joinToString(
                    separator = ",\n                                    ",
                    prefix = "$className(\n                                    ",
                    postfix = ",\n                                )",
                    transform = CompositeLiteralParserPair::constructorParameter,
                )
                val parserBody = "$variables$returnExpression"
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
