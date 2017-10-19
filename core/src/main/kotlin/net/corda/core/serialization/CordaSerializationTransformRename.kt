package net.corda.core.serialization

/**
 * This annotation is used to mark a class as having had multiple elements renamed as a container annotation for
 * instances of [CordaSerializationTransformRename], each of which details an individual rename.
 *
 * @property value an array of [CordaSerializationTransformRename]
 *
 * NOTE: Order is important, new values should always be added before existing
 *
 * IMPORTANT - Once added (and in production) do NOT remove old annotations. See documentation for
 * more discussion on this point!.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class CordaSerializationTransformRenames(vararg val value: CordaSerializationTransformRename)

/**
 * This annotation is used to mark a class has having had an element renamed. It is used by the
 * AMQP deserialiser to allow instances with different versions of the class on their Class Path
 * to successfully deserialize the object
 *
 * @property to [String] representation of the properties new name
 * @property from [String] representation of the properties old new
 *
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
// When Kotlin starts writing 1.8 class files enable this, it removes the need for the wrapping annotation
//@Repeatable
annotation class CordaSerializationTransformRename(val to: String, val from: String)
