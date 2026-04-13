package net.aholbrook.kotlinleaderelection

@Retention(AnnotationRetention.RUNTIME)
@Target(
    AnnotationTarget.CLASS,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.CONSTRUCTOR,
)
internal annotation class ExcludeFromJacocoGeneratedReport

@RequiresOptIn(
    message = "This is an internal kotlin-leader-election API and subject to change without notice.",
    level = RequiresOptIn.Level.ERROR,
)
@Retention(AnnotationRetention.BINARY)
@Target(
    AnnotationTarget.CLASS,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.PROPERTY,
    AnnotationTarget.FIELD,
    AnnotationTarget.TYPEALIAS,
    AnnotationTarget.CONSTRUCTOR,
)
internal annotation class InternalApi
