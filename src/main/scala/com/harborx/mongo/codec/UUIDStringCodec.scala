package com.harborx.mongo.codec

import java.util.UUID

import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class UUIDStringCodec extends Codec[UUID]{
  override def decode(reader: BsonReader, decoderContext: DecoderContext): UUID = {
    UUID.fromString(reader.readString())
  }

  override def encode(writer: BsonWriter, value: UUID, encoderContext: EncoderContext) = {
    writer.writeString(value.toString)
  }

  override def getEncoderClass = classOf[UUID]
}
