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
import com.google.devtools.ksp.symbol.KSVisitorVoid
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.validate
import org.snappy.ksp.appendText
import org.snappy.ksp.hasAnnotation
import org.snappy.ksp.isInstance
import org.snappy.postgresql.type.PgType
import org.snappy.postgresql.type.ToPgObject

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
        val visitors = resolver.getSymbolsWithAnnotation(PgType::class.java.name)
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
                if (hasEncoder || !encoderNeeded) {
                    return@mapNotNull null
                }
                val visitor = PgTypeEncoderVisitor()
                annotated.accept(visitor, Unit)
                visitor
            }
            .toList()
        if (visitors.isNotEmpty()) {
            processEncodeTypes(visitors)
        }
        hasRun = true
        return emptyList()
    }

    private fun processEncodeTypes(encodeTypes: List<PgTypeEncoderVisitor>) {
        val file = try {
            codeGenerator.createNewFile(
                dependencies = Dependencies(
                    true,
                    *encodeTypes.map { it.classDeclaration.containingFile!! }.toTypedArray(),
                ),
                packageName = DESTINATION_PACKAGE,
                fileName = "Encoders",
            )
        } catch (ex: FileAlreadyExistsException) {
            logger.info("Skipping creation since file already exists")
            return
        }

        val encodeMethods = encodeTypes.joinToString(
            separator = "\n\n            ",
            transform = PgTypeEncoderVisitor::encodeMethod,
        )

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

    inner class PgTypeEncoderVisitor : KSVisitorVoid() {
        private val appendPropertyCalls = mutableListOf<String>()
        lateinit var classDeclaration: KSClassDeclaration
        private lateinit var className: String
        private lateinit var classPackage: String
        private lateinit var typeName: String

        fun encodeMethod(): String {
            val encodeMethod = appendPropertyCalls.joinToString(separator = "\n                ")
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

        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            this.classDeclaration = classDeclaration
            classPackage = classDeclaration.packageName.asString()
            className = classDeclaration.simpleName.asString()
            val annotationValue = classDeclaration.annotations
                .first { it.isInstance<PgType>() }
                .arguments[0]
                .value as String
            typeName = annotationValue.replace("\"", "\"\"")
            imports += "$classPackage.$className"

            when (classDeclaration.classKind) {
                ClassKind.ENUM_CLASS -> appendPropertyCalls += "val encodedValue = this@encode.name"
                ClassKind.CLASS -> {
                    appendPropertyCalls += "val builder = PgCompositeLiteralBuilder()"
                    classDeclaration.getDeclaredProperties()
                        .filter { it.getter != null }
                        .forEach { it.accept(this, data) }
                    appendPropertyCalls += "val encodedValue = builder.toString()"
                }
                else -> error("PgType can only be attached to an enum class, data class or plain class")
            }
        }

        override fun visitPropertyDeclaration(property: KSPropertyDeclaration, data: Unit) {
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
            appendPropertyCalls += "builder.append$readType(this.$name$methodCall)"
        }
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