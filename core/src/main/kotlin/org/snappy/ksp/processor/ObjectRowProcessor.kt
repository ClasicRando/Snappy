package org.snappy.ksp.processor

import com.google.devtools.ksp.getAllSuperTypes
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.validate
import org.snappy.copy.ToObjectRow
import org.snappy.ksp.appendText
import org.snappy.ksp.symbols.ObjectRow

class ObjectRowProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
) : SymbolProcessor {
    private val imports = mutableSetOf<String>()
    private var hasRun = false

    override fun process(resolver: Resolver): List<KSAnnotated> {
        if (hasRun) {
            return emptyList()
        }
        val encoders = resolver.getSymbolsWithAnnotation(ObjectRow::class.java.name)
            .filter { it is KSClassDeclaration && it.validate() }
            .mapNotNull { annotated ->
                annotated as KSClassDeclaration
                val hasEncoder = annotated.getAllSuperTypes().any {
                    it.declaration.simpleName.asString() == ToObjectRow::class.simpleName!!
                }
                if (hasEncoder) null else annotated
            }
            .toList()
        if (encoders.isNotEmpty()) {
            processObjectRowTypes(encoders)
        }
        hasRun = true
        return emptyList()
    }

    private fun getObjectRowMethod(objectRowClass: KSClassDeclaration): String {
        val classPackage = objectRowClass.packageName.asString()
        val className = objectRowClass.simpleName.asString()
        val parameters = objectRowClass.primaryConstructor!!.parameters.joinToString(
            prefix = "listOf(\n                        ",
            separator = ",\n                        ",
            postfix = ",\n                    )"
        ) { param ->
            val hasBulkCopyModifier = objectRowClass.getAllFunctions().any {
                it.simpleName.asString() == "bulkCopyString"
            }
            val additionalModifier = when {
                hasBulkCopyModifier -> ".bulkCopyString()"
                else -> ""
            }
            "obj.${param.name!!.asString()}$additionalModifier"
        }

        imports += "$classPackage.$className"
        return """
            fun Sequence<$className>.mapToObjectRow(): Sequence<ToObjectRow> = map { obj ->
                ToObjectRow {
                    $parameters
                }
            }
        """.trim()
    }

    private fun processObjectRowTypes(objectRowTypes: List<KSClassDeclaration>) {
        val file = try {
            codeGenerator.createNewFile(
                dependencies = Dependencies(
                    true,
                    *objectRowTypes.map { it.containingFile!! }.toTypedArray(),
                ),
                packageName = DESTINATION_PACKAGE,
                fileName = "Generators",
            )
        } catch (ex: FileAlreadyExistsException) {
            logger.info("Skipping creation since file already exists")
            return
        }

        val encodeMethods = objectRowTypes.joinToString("\n\n            ") {
            getObjectRowMethod(it)
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
        logger.info("Created Generator file for object row sequence extensions")
    }

    companion object {
        const val DESTINATION_PACKAGE: String = "org.snappy.copy"
    }
}