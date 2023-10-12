package org.snappy.postgresql.ksp.processor

import com.google.devtools.ksp.getAllSuperTypes
import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.validate
import org.snappy.ksp.appendText
import org.snappy.ksp.hasAnnotation
import org.snappy.ksp.isInstance
import org.snappy.postgresql.type.PgType
import org.snappy.postgresql.type.ToPgObject
import javax.swing.text.StyledEditorKit.BoldAction

class PgTypeEncoderProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
) : SymbolProcessor {
    private val imports = mutableSetOf(
        "org.snappy.postgresql.type.ToPgObject",
        "org.snappy.postgresql.literal.PgCompositeLiteralBuilder",
        "org.postgresql.util.PGobject",
    )
    private var hasRun = false

    override fun process(resolver: Resolver): List<KSAnnotated> {
        if (hasRun) {
            return emptyList()
        }
        val encoders = resolver.getSymbolsWithAnnotation(PgType::class.java.name)
            .filter { it is KSClassDeclaration && it.validate() }
            .mapNotNull { annotated ->
                annotated as KSClassDeclaration
                val hasEncoder = annotated.getAllSuperTypes().any {
                    it.declaration.simpleName.asString() == ToPgObject::class.simpleName!!
                }
                val encoderNeeded = annotated.annotations
                    .first { it.isInstance<PgType>() }
                    .arguments[3]
                    .value as Boolean
                if (hasEncoder || !encoderNeeded) null else annotated
            }
            .toList()
        if (encoders.isNotEmpty()) {
            processEncodeTypes(encoders)
        }
        hasRun = true
        return emptyList()
    }

    private fun createAppendCallFromProperty(property: KSPropertyDeclaration): String {
        val name = property.simpleName.asString()
        val propertyTypeDeclaration = property.type.resolve().declaration as KSClassDeclaration
        val simpleName = propertyTypeDeclaration.simpleName.asString()
        var methodCall = ""

        val readType = when {
            propertyTypeDeclaration.hasAnnotation<PgType>() -> {
                val implementsToPgObject = propertyTypeDeclaration.getAllSuperTypes().any {
                    it.declaration.simpleName.asString() == ToPgObject::class.simpleName!!
                }
                methodCall = if (implementsToPgObject) "" else ".encode()"
                "Composite"
            }
            propertyTypeDeclaration.classKind == ClassKind.ENUM_CLASS -> {
                "Enum"
            }
            simpleName == "PgJson" -> {
                "Composite"
            }
            propertyTypeDeclaration.modifiers.any { it == Modifier.VALUE } -> {
                val parameter = propertyTypeDeclaration.primaryConstructor!!.parameters.first()
                val parameterTypeDeclaration = parameter.type.resolve().declaration as KSClassDeclaration
                val parameterTypeName = parameterTypeDeclaration.simpleName.asString()
                if (parameterTypeName !in validAppendTypes) {
                    error("Value class must have valid append type")
                }
                if (
                    propertyTypeDeclaration.getDeclaredProperties().none { prop ->
                        prop.simpleName.asString() == "value" && prop.modifiers.none {
                            it == Modifier.PRIVATE || it == Modifier.PROTECTED
                        }
                    }
                ) {
                    error("Value class properties must have a public value property")
                }
                methodCall = ".value"
                parameterTypeName
            }
            propertyTypeDeclaration.simpleName.asString() == "List" -> "Iterable"
            simpleName in validAppendTypes -> simpleName
            else -> error("Property $name is not able to be parsed into a composite literal")
        }
        return "builder.append$readType(this.$name$methodCall)"
    }

    private fun getCompositeEncoderMethod(encodeClass: KSClassDeclaration): String {
        val propertyAppendCalls = encodeClass.getDeclaredProperties()
            .filter { it.getter != null }
            .map { createAppendCallFromProperty(it) }
            .joinToString("\n                ")
        return """
                val builder = PgCompositeLiteralBuilder()
                $propertyAppendCalls
                val encodedValue = builder.toString()
        """.trim()
    }

    private fun getEncoderMethod(encodeClass: KSClassDeclaration): String {
        val classPackage = encodeClass.packageName.asString()
        val className = encodeClass.simpleName.asString()
        val annotationValue = encodeClass.annotations
            .first { it.isInstance<PgType>() }
            .arguments[0]
            .value as String
        val typeName = annotationValue.replace("\"", "\"\"")

        val encodeMethod = when (encodeClass.classKind) {
            ClassKind.ENUM_CLASS -> "val encodedValue = this@encode.name"
            ClassKind.CLASS -> getCompositeEncoderMethod(encodeClass)
            else -> error("PgType can only be attached to an enum class, data class or plain class")
        }

        imports += "$classPackage.$className"
        return """
            fun $className.encode() = ToPgObject {
                $encodeMethod
                PGobject().apply {
                    value = encodedValue
                    type = "$typeName"
                }
            }
        """.trim()
    }

    private fun processEncodeTypes(encodeTypes: List<KSClassDeclaration>) {
        val file = try {
            codeGenerator.createNewFile(
                dependencies = Dependencies(
                    true,
                    *encodeTypes.map { it.containingFile!! }.toTypedArray(),
                ),
                packageName = DESTINATION_PACKAGE,
                fileName = "Encoders",
            )
        } catch (ex: FileAlreadyExistsException) {
            logger.info("Skipping creation since file already exists")
            return
        }

        val encodeMethods = encodeTypes.joinToString("\n\n            ") {
            getEncoderMethod(it)
        }

        val importsOrdered = imports.sorted().joinToString(
            separator = "\n            ",
        ) {
            "import $it"
        }
        file.appendText("""
            @file:Suppress("UNUSED")
            package $DESTINATION_PACKAGE
            
            $importsOrdered
            
            $encodeMethods
            
        """.trimIndent())
        logger.info("Created encoder file for encoder extensions")
    }

    companion object {
        const val DESTINATION_PACKAGE: String = "org.snappy.postgresql.composite.encoders"
        val validAppendTypes = listOf(
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
            "Array",
        )
    }
}