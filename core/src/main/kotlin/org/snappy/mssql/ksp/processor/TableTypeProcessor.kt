package org.snappy.mssql.ksp.processor

import com.google.devtools.ksp.getAllSuperTypes
import com.google.devtools.ksp.getDeclaredProperties
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
import com.google.devtools.ksp.symbol.Modifier
import com.google.devtools.ksp.validate
import org.snappy.ksp.appendText
import org.snappy.ksp.isInstance
import org.snappy.ksp.symbols.Rename
import org.snappy.mssql.ksp.symbol.TableType

class TableTypeProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
) : SymbolProcessor {
    override fun process(resolver: Resolver): List<KSAnnotated> {
        val symbols = resolver.getSymbolsWithAnnotation(TableType::class.java.name)
        val result = symbols.filter { !it.validate() }.toList()
        symbols.filter {
            it is KSClassDeclaration && it.validate()
        }.forEach {
            it.accept(TableTypeVisitor(), Unit)
        }
        return result
    }

    private val columnKClass = Rename::class

    inner class TableTypeVisitor : KSVisitorVoid() {
        inner class ParameterDetails(
            parameter: KSValueParameter,
            propertyAnnotations: Map<String, String>,
        ) {
            val paramName = parameter.name!!.asString()
            val fieldName = propertyAnnotations[paramName] ?: paramName
            private val paramType = parameter.type.resolve()
            private val paramDeclaration = paramType.declaration as KSClassDeclaration
            private val paramDeclarationName = paramDeclaration.simpleName.asString()
            val sqlTypeId = when (paramDeclarationName) {
                "Boolean" -> "java.sql.Types.BIT"
                "Short" -> "java.sql.Types.SMALLINT"
                "Int" -> "java.sql.Types.INTEGER"
                "Long" -> "java.sql.Types.BIGINT"
                "Float" -> "java.sql.Types.REAL"
                "Double" -> "java.sql.Types.DOUBLE"
                "String" -> "java.sql.Types.VARCHAR"
                "BigDecimal" -> "java.sql.Types.NUMERIC"
                "Date" -> "java.sql.Types.DATE"
                "Time" -> "java.sql.Types.TIME"
                "DateTime" -> "microsoft.sql.Types.DATETIME"
                "SmallDateTime" -> "microsoft.sql.Types.SMALLDATETIME"
                "DateTimeOffset" -> "microsoft.sql.Types.DATETIMEOFFSET"
                "LocalDate" -> "java.sql.Types.DATE"
                "LocalTime" -> "java.sql.Types.TIME"
                "LocalDateTime" -> "java.sql.Types.TIMESTAMP"
                "OffsetDateTime" -> "java.sql.Types.TIMESTAMP"
                "Instant" -> "java.sql.Types.TIMESTAMP"
                "Timestamp" -> "java.sql.Types.TIMESTAMP"
                else -> "java.sql.Types.OTHER"
            }
        }

        private val imports = mutableSetOf(
            "org.snappy.encode.Encode",
            "com.microsoft.sqlserver.jdbc.SQLServerDataTable",
            "java.sql.PreparedStatement",
        )

        private fun addImport(import: String) {
            if (import.startsWith("kotlin.")) return
            imports += import
        }

        private val importsSorted: List<String> get() = imports.sorted()

        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            classDeclaration.primaryConstructor!!.accept(this, data)
        }

        private fun generateTableType(
            constructor: KSFunctionDeclaration,
            classDeclaration: KSClassDeclaration,
        ) {
            val classPackage = classDeclaration.packageName.asString()
            val className = classDeclaration.simpleName.asString()
            val tableTypeName = "${className}TableType"
            val file = codeGenerator.createNewFile(
                dependencies = Dependencies(true, constructor.containingFile!!),
                packageName = DESTINATION_PACKAGE,
                fileName = tableTypeName,
            )
            val tableTypeDbName = classDeclaration.annotations
                .first { it.isInstance<TableType>() }
                .arguments[0]
                .value as String
            addImport("$classPackage.$className")

            val propertyAnnotations = classDeclaration.getDeclaredProperties()
                .mapNotNull { prop ->
                    val name = prop.annotations.filter {
                        val annotationDeclaration = it.annotationType.resolve().declaration
                        it.shortName.getShortName() == columnKClass.simpleName
                                && annotationDeclaration.qualifiedName?.asString() == columnKClass.qualifiedName
                    }.map {
                        it.arguments[0].value as String
                    }.firstOrNull() ?: return@mapNotNull null
                    prop.simpleName.asString() to name
                }
                .toMap()
            val parameters = constructor.parameters.map { parameter ->
                ParameterDetails(parameter, propertyAnnotations)
            }

            val isToTvpRow = classDeclaration.getAllSuperTypes()
                .any { it.declaration.simpleName.asString() == "ToTvpRow" }
            val rowToItems = if (isToTvpRow) {
                "val items = row.toTvpRow()"
            } else {
                parameters.joinToString(
                    prefix = "val items = arrayOf<Any?>(\n    ",
                    separator = ",\n    ",
                    postfix = ",\n)",
                ) { "row.${it.paramName}" }
            }.replaceIndent("                            ").trim()

            val columnMetadata = parameters.joinToString(
                separator = "\n",
            ) { parameter ->
                "addColumnMetadata(\"${parameter.fieldName}\", ${parameter.sqlTypeId})"
            }.replaceIndent("                        ").trim()

            val importsOrdered = importsSorted.joinToString(
                separator = "\n                ",
            ) {
                "import $it"
            }
            file.appendText("""
                @file:Suppress("UNUSED")
                package $DESTINATION_PACKAGE
                
                $importsOrdered
                
                class $tableTypeName(rows: Iterable<$className>) : Encode {
                    private val data = SQLServerDataTable().apply {
                        tvpName = "$tableTypeDbName"
                        $columnMetadata
                        var hasCheckedSize = false
                        for (row in rows) {
                            $rowToItems
                            if (!hasCheckedSize) {
                                check(items.size == ${parameters.size})
                                hasCheckedSize = true
                            }
                            addRow(*items)
                        }
                    }
                    
                    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
                        preparedStatement.setObject(parameterIndex, data)
                    }
                }
                
            """.trimIndent())
        }

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            val classDeclaration = function.parentDeclaration as? KSClassDeclaration ?: return
            val simpleName = classDeclaration.simpleName.asString()
            if (classDeclaration.typeParameters.any()) {
                error("Cannot generate table type for class with generic parameters, '$simpleName'")
            }
            if (!classDeclaration.modifiers.contains(Modifier.DATA)) {
                error("Cannot generate table type for class that is not a data class '$simpleName'")
            }
            generateTableType(function, classDeclaration)
        }
    }

    companion object {
        const val DESTINATION_PACKAGE: String = "org.snappy.mssql.tvp.types"
    }
}