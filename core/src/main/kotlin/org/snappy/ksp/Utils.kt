package org.snappy.ksp

import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSDeclaration
import java.io.OutputStream

/** */
fun OutputStream.appendText(str: String) {
    this.write(str.toByteArray())
}

/**
 *
 */
inline fun <reified T> KSDeclaration.hasAnnotation(): Boolean {
    return annotations.any { it.isInstance<T>() }
}

/**
 *
 */
inline fun <reified T> KSAnnotation.isInstance(): Boolean {
    return this.shortName.asString() == T::class.simpleName
}
