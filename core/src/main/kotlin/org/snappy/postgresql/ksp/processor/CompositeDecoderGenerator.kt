package org.snappy.postgresql.ksp.processor

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.Modifier
import org.snappy.ksp.AbstractFileGenerator
import org.snappy.ksp.appendText
import org.snappy.ksp.hasAnnotation
import org.snappy.postgresql.type.PgType

class CompositeDecoderGenerator(
    private val constructorFunction: KSFunctionDeclaration,
    private val classDeclaration: KSClassDeclaration,
    private val logger: KSPLogger,
) : AbstractFileGenerator() {
    init {
        addImport("org.snappy.postgresql.type.PgObjectDecoder")
        addImport("org.postgresql.util.PGobject")
    }

    private fun createVariableDeclarationFromParameter(
        parameter: KSValueParameter,
    ): String {
        val name = parameter.name?.asString()
            ?: error("Could not find name for ${parameter.name?.asString()}")
        val parameterType = parameter.type.resolve()
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
                val valueParameter = parameterTypeDeclaration.primaryConstructor!!
                    .parameters
                    .first()
                val valueParameterTypeDeclaration = valueParameter.type
                    .resolve().declaration as KSClassDeclaration
                val parameterTypeName = valueParameterTypeDeclaration.simpleName.asString()
                if (parameterTypeName !in PgTypeEncoderProcessor.validAppendTypes) {
                    error("Value class must have valid append type")
                }
                addImport("$packageName.$simpleName")
                return "val $name = $simpleName(read$parameterTypeName()$nullCheck)"
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
        return "val $name = read$readType$nullCheck"
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
        addImport("org.snappy.postgresql.literal.parseComposite")
        val parserBody = getCompositeLiteralParser(classDeclaration, constructorFunction)
        val importsOrdered = importsSorted.joinToString(
            separator = "\n            ",
        ) {
            "import $it"
        }
        file.appendText("""
            @file:Suppress("UNUSED")
            package ${PgTypeDecoderProcessor.DESTINATION_PACKAGE}
            
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

    companion object {
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