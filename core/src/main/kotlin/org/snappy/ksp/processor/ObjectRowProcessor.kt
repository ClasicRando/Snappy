package org.snappy.ksp.processor

import com.google.devtools.ksp.getAllSuperTypes
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.KSVisitorVoid
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
        logger.warn(resolver.getSymbolsWithAnnotation(ObjectRow::class.java.name).joinToString { (it as KSClassDeclaration).simpleName.asString() })
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
                if (hasEncoder) {
                    return@mapNotNull null
                }
                val visitor = ObjectRowVisitor()
                annotated.accept(visitor, Unit)
                visitor
            }
            .toList()
        if (encoders.isNotEmpty()) {
            processObjectRowTypes(encoders)
        }
        hasRun = true
        return emptyList()
    }

    inner class ObjectRowVisitor : KSVisitorVoid() {
        lateinit var classDeclaration: KSClassDeclaration
        private lateinit var className: String
        private lateinit var classPackage: String
        private val objectList = mutableListOf<String>()

        fun objectRowMethod(): String {
            val parameters = objectList.joinToString(
                prefix = "listOf(\n                        ",
                separator = ",\n                        ",
                postfix = ",\n                    )",
            )
            imports += "$classPackage.$className"
            return """
            fun Sequence<$className>.mapToObjectRow(): Sequence<ToObjectRow> = map { obj ->
                ToObjectRow {
                    $parameters
                }
            }
        """.trim()
        }

        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            this.classDeclaration = classDeclaration
            className = classDeclaration.simpleName.asString()
            classPackage = classDeclaration.packageName.asString()
            classDeclaration.primaryConstructor!!.accept(this, data)
        }

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            for (parameter in function.parameters) {
                parameter.accept(this, data)
            }
        }

        override fun visitValueParameter(valueParameter: KSValueParameter, data: Unit) {
            val parameterClassDeclaration = valueParameter.type
                .resolve()
                .declaration as KSClassDeclaration
            val hasBulkCopyModifier = parameterClassDeclaration.getAllFunctions().any {
                it.simpleName.asString() == "bulkCopyString"
            }
            val additionalModifier = when {
                hasBulkCopyModifier -> ".bulkCopyString()"
                else -> ""
            }
            objectList += "obj.${valueParameter.name!!.asString()}$additionalModifier"
        }
    }

    private fun processObjectRowTypes(objectRowTypes: List<ObjectRowVisitor>) {
        val file = codeGenerator.createNewFile(
            dependencies = Dependencies(
                true,
                *objectRowTypes.map { it.classDeclaration.containingFile!! }.toTypedArray(),
            ),
            packageName = DESTINATION_PACKAGE,
            fileName = "Generators",
        )

        val encodeMethods = objectRowTypes.joinToString("\n\n            ") {
            it.objectRowMethod()
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