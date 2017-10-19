package net.corda.nodeapi.internal.serialization.amqp

import org.apache.qpid.proton.amqp.DescribedType
import org.apache.qpid.proton.codec.Data
import org.apache.qpid.proton.codec.DescribedTypeConstructor

import java.io.NotSerializableException

/**
 * This class wraps all serialized data, so that the schema can be carried along with it.  We will provide various
 * internal utilities to decompose and recompose with/without schema etc so that e.g. we can store objects with a
 * (relationally) normalised out schema to avoid excessive duplication.
 */
// TODO: make the schema parsing lazy since mostly schemas will have been seen before and we only need it if we
// TODO: don't recognise a type descriptor.
data class Envelope(val obj: Any?, val schema: Schema, val transformsSchema: TransformsSchema) : DescribedType {
    companion object : DescribedTypeConstructor<Envelope> {
        val DESCRIPTOR = AMQPDescriptorRegistry.ENVELOPE.amqpDescriptor
        val DESCRIPTOR_OBJECT = Descriptor(null, DESCRIPTOR)

        // described list should either be two or three elements long
        private const val envelopeWithTransforms = 3
        private const val envelopeWithoutTransforms = 2

        private const val blobIdx = 0
        private const val schemaIdx = 1
        private const val transformSchemaIdx = 2

        fun get(data: Data): Envelope {
            val describedType = data.`object` as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            val list = describedType.described as List<*>

            // We need to cope with objects serialised without the transforms header element in the
            // envelope
            val transformSchema: Any? = when (list.size) {
                envelopeWithoutTransforms -> null
                envelopeWithTransforms -> list[transformSchemaIdx]
                else -> throw NotSerializableException("Malformed list, bad length of ${list.size} (should be 2 or 3)")
            }

            return newInstance(listOf(list[blobIdx], Schema.get(list[schemaIdx]!!),
                    TransformsSchema.newInstance(transformSchema)))
        }

        // This separation of functions is needed as this will be the entry point for the default
        // AMQP decoder if one is used (see the unit tests).
        override fun newInstance(described: Any?): Envelope {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")

            // We need to cope with objects serialised without the transforms header element in the
            // envelope
            val transformSchema = when (list.size) {
                envelopeWithoutTransforms -> TransformsSchema.newInstance(null)
                envelopeWithTransforms -> list[transformSchemaIdx] as TransformsSchema
                else -> throw NotSerializableException("Malformed list, bad length of ${list.size} (should be 2 or 3)")
            }
            return Envelope(list[blobIdx], list[schemaIdx] as Schema, transformSchema)
        }

        override fun getTypeClass(): Class<*> = Envelope::class.java
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(obj, schema, transformsSchema)
}
