package org.snappy.postgresql.ksp.processor

import com.google.devtools.ksp.getAllSuperTypes
import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
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
        "org.snappy.postgresql.type.PgCompositeLiteralBuilder",
        "org.postgresql.util.PGobject",
    )

    override fun process(resolver: Resolver): List<KSAnnotated> {
        val encoders = resolver.getSymbolsWithAnnotation(PgType::class.java.name)
            .filter { it is KSClassDeclaration && it.validate() }
            .mapNotNull { annotated ->
                annotated as KSClassDeclaration
                val hasEncoder = annotated.getAllSuperTypes().any {
                    it.declaration.simpleName.asString() == ToPgObject::class.simpleName!!
                }
                if (hasEncoder) null else annotated
            }
            .toList()
        processEncodeTypes(encoders)
        return emptyList()
    }

    private fun createAppendCallFromProperty(property: KSPropertyDeclaration): String {
        val name = property.simpleName.asString()
        val propertyTypeDeclaration = property.type.resolve().declaration
        val simpleName = propertyTypeDeclaration.simpleName.asString()

        val readType = when {
            propertyTypeDeclaration.hasAnnotation<PgType>() -> "Composite"
            propertyTypeDeclaration.simpleName.asString() == "List" -> "Iterable"
            simpleName in PgTypeDecoderProcessor.validAppendTypes -> simpleName
            else -> error("Property $name is not able to be parsed into a composite literal")
        }
        return "builder.append$readType(this.$name)"
    }

    private fun getEncoderMethod(encodeClass: KSClassDeclaration): String {
        val classPackage = encodeClass.packageName.asString()
        val className = encodeClass.simpleName.asString()
        val annotationValue = encodeClass.annotations
            .first { it.isInstance<PgType>() }
            .arguments[0]
            .value as String
        val typeName = annotationValue.replace("\"", "\"\"")

        val propertyAppendCalls = encodeClass.getDeclaredProperties()
            .filter { it.getter != null }
            .map { createAppendCallFromProperty(it) }
            .joinToString("\n                ")

        imports += "$classPackage.$className"
        return """
            fun $className.encode() = ToPgObject {
                val builder = PgCompositeLiteralBuilder()
                $propertyAppendCalls
                
                PGobject().apply {
                    value = builder.toString()
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
        logger.warn("Created encoder file for encoder extensions")
    }
    companion object {
        const val DESTINATION_PACKAGE: String = "org.snappy.postgresql.composite.encoders"
    }
}