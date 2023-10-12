package org.snappy.ksp.processor

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSVisitorVoid
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.validate
import org.snappy.ksp.symbols.Flatten
import org.snappy.ksp.symbols.Rename
import org.snappy.ksp.symbols.RowParser
import kotlin.reflect.KClass

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

    inner class RowParserVisitor : KSVisitorVoid() {
        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            classDeclaration.primaryConstructor!!.accept(this, data)
        }

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            val classDeclaration = function.parentDeclaration as? KSClassDeclaration ?: return
            val simpleName = classDeclaration.simpleName.asString()
            if (classDeclaration.typeParameters.any()) {
                error("Cannot generate row parser for class with generic parameters, '$simpleName'")
            }
            if (classDeclaration.modifiers.contains(Modifier.DATA)) {
                DataClassRowParserGenerator(classDeclaration, function).generateFile(codeGenerator)
                logger.info("Created data class row parser for '$simpleName'")
                return
            }
            if (function.parameters.any()) {
                error("Cannot generate Pojo row parser if constructor has parameters, '$simpleName'")
            }
            PojoClassRowParserGenerator(classDeclaration).generateFile(codeGenerator)
            logger.info("Created pojo class row parser for '$simpleName'")
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