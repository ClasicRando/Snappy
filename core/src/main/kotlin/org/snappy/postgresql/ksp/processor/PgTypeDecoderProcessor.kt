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
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.validate
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

    inner class CompositeVisitor : KSVisitorVoid() {
        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            classDeclaration.primaryConstructor!!.accept(this, data)
        }

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            val classDeclaration = function.parentDeclaration as? KSClassDeclaration ?: return
            val annotationArguments = classDeclaration.annotations
                .first { it.isInstance<PgType>() }
                .arguments
            val createArrayDecoder = annotationArguments[1].value as Boolean
            val createDecoder = annotationArguments[2].value as Boolean
            if (!createDecoder) {
                return
            }

            val isEnum = classDeclaration.classKind == ClassKind.ENUM_CLASS
            if (!classDeclaration.modifiers.contains(Modifier.DATA) && !isEnum) {
                error("Only data classes can be used as postgresql type")
            }
            if (classDeclaration.typeParameters.any()) {
                error("Generic PgType classes are not supported")
            }
            if (isEnum) {
                EnumDecoderGenerator(function, classDeclaration, logger).generateFile(codeGenerator)
            } else {
                CompositeDecoderGenerator(function, classDeclaration, logger).generateFile(codeGenerator)
            }
            if (createArrayDecoder) {
                ArrayDecoderGenerator(function, classDeclaration, logger).generateFile(codeGenerator)
            }
        }
    }

    companion object {
        const val DESTINATION_PACKAGE: String = "org.snappy.postgresql.composite.decoders"
    }
}
