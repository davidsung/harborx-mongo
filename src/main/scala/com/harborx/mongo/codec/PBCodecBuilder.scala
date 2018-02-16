package com.harborx.mongo.codec

import com.google.protobuf.ByteString
import com.trueaccord.scalapb.json.JsonFormat
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonType, BsonWriter}
import org.mongodb.scala.bson.BsonBinary

import scalapb.descriptors._

private[codec] class BsonFormatException(msg: String, cause: Exception) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}

class PBCodecBuilder(includingDefaultValueFields: Boolean = false,
                     preservingProtoFieldNames: Boolean = false,
                     formattingLongAsNumber: Boolean = true) {
  def getCodecFor[A <: GeneratedMessage with Message[A]](clazz: Class[A])(implicit cmp: GeneratedMessageCompanion[A]): Codec[A] = {
    new Codec[A] { // big class!!
      override def encode(writer: BsonWriter, value: A, encoderContext: EncoderContext): Unit = {
        _encode(writer, value)
      }

      override def getEncoderClass: Class[A] = clazz

      override def decode(reader: BsonReader, decoderContext: DecoderContext): A = {
        cmp.messageReads.read(fromReaderToPMessage(cmp, reader))
      }

      private def defaultEnumParser(enumDescriptor: EnumDescriptor, reader: BsonReader): EnumValueDescriptor = {
        reader.getCurrentBsonType() match {
          case BsonType.INT32 =>
            val value = reader.readInt32()
            enumDescriptor.findValueByNumber(value)
              .getOrElse(throw new BsonFormatException(s"Invalid enum value: $value for enum type: ${enumDescriptor.fullName}"))
          case BsonType.STRING =>
            val value = reader.readString()
            enumDescriptor.values.find(_.name == value)
              .getOrElse(throw new BsonFormatException(s"Unrecognized enum value '$value'"))
          case t =>
            throw new BsonFormatException(s"Unexpected value with type ($t) for enum ${enumDescriptor.fullName}")
        }
      }

      private def parseSingleValue(containerCompanion: GeneratedMessageCompanion[_], fd: FieldDescriptor, reader: BsonReader): PValue = fd.scalaType match {
        case ScalaType.Enum(ed) =>
          PEnum(defaultEnumParser(ed, reader))
        case ScalaType.Message(md) =>
          fromReaderToPMessage(containerCompanion.messageCompanionForFieldNumber(fd.number), reader)
        case st =>
          parsePrimitiveByScalaType(st, reader,
            throw new BsonFormatException(s"Unexpected value with type (${reader.getCurrentBsonType})for field ${serializedName(fd)} of ${fd.containingMessage.name}"))
      }

      private def parsePrimitiveByScalaType(scalaType: ScalaType, reader: BsonReader, onError: => PValue): PValue = {
        (scalaType, reader.getCurrentBsonType()) match {
          case (ScalaType.Int, BsonType.INT32) => PInt(reader.readInt32())
          case (ScalaType.Int, BsonType.DOUBLE) => PInt(reader.readDouble().toInt)
          case (ScalaType.Int, BsonType.NULL) => reader.readNull(); PInt(0)
          case (ScalaType.Long, BsonType.INT64) => PLong(reader.readInt64())
          case (ScalaType.Long, BsonType.STRING) => PLong(reader.readString().toLong)
          case (ScalaType.Long, BsonType.INT32) => PLong(reader.readInt32())
          case (ScalaType.Long, BsonType.NULL) => reader.readNull(); PLong(0L)
          case (ScalaType.Double, BsonType.DOUBLE) => PDouble(reader.readDouble())
          case (ScalaType.Double, BsonType.INT32) => PDouble(reader.readInt32())
          case (ScalaType.Double, BsonType.INT64) => PDouble(reader.readInt64())
          case (ScalaType.Double, BsonType.STRING) =>
            val str = reader.readString()
            str match {
              case "NaN" => PDouble(Double.NaN)
              case "Infinity" => PDouble(Double.PositiveInfinity)
              case "-Infinity" => PDouble(Double.NegativeInfinity)
              case _ => PDouble(Double.NaN)
            }
          case (ScalaType.Double, BsonType.NULL) => reader.readNull(); PDouble(0.toDouble)
          case (ScalaType.Float, BsonType.DOUBLE) => PFloat(reader.readDouble().toFloat)
          case (ScalaType.Float, BsonType.INT32) => PFloat(reader.readInt32)
          case (ScalaType.Float, BsonType.INT64) => PFloat(reader.readInt64())
          case (ScalaType.Float, BsonType.STRING) =>
            val str = reader.readString()
            str match {
              case "NaN" => PFloat(Float.NaN)
              case "Infinity" => PFloat(Float.PositiveInfinity)
              case "-Infinity" => PFloat(Float.NegativeInfinity)
              case _ => PFloat(Float.NaN)
            }
          case (ScalaType.Float, BsonType.NULL) => reader.readNull(); PFloat(0.toFloat)
          case (ScalaType.Boolean, BsonType.BOOLEAN) => PBoolean(reader.readBoolean())
          case (ScalaType.Boolean, BsonType.NULL) => reader.readNull(); PBoolean(false)
          case (ScalaType.String, BsonType.STRING) => PString(reader.readString())
          case (ScalaType.String, BsonType.NULL) => reader.readNull(); PString("")
          case (ScalaType.ByteString, BsonType.BINARY) =>
            PByteString(ByteString.copyFrom(reader.readBinaryData().getData))
          case (ScalaType.ByteString, BsonType.NULL) => reader.readNull(); PByteString(ByteString.EMPTY)
          case _ => onError
        }
      }

      private def fromReaderToPMessage(cmp: GeneratedMessageCompanion[_], reader: BsonReader): PMessage = {
        def parseValue(maps: Map[String, FieldDescriptor], reader: BsonReader): Option[(FieldDescriptor, PValue)] = {
          val name = reader.readName()
          maps.get(name) match {
            case Some(fd) =>
              if (fd.isMapField) {
                reader.getCurrentBsonType match {
                  case BsonType.DOCUMENT =>
                    val mapEntryDesc = fd.scalaType.asInstanceOf[ScalaType.Message].descriptor
                    val keyDescriptor = mapEntryDesc.findFieldByNumber(1).get
                    val valueDescriptor = mapEntryDesc.findFieldByNumber(2).get
                    var vec = Vector[PValue]()
                    reader.readStartDocument()
                    // start while
                    while(reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                      val key = reader.readName()
                      val keyObj = keyDescriptor.scalaType match {
                        case ScalaType.Boolean => PBoolean(java.lang.Boolean.valueOf(key))
                        case ScalaType.Double => PDouble(java.lang.Double.valueOf(key))
                        case ScalaType.Float => PFloat(java.lang.Float.valueOf(key))
                        case ScalaType.Int => PInt(java.lang.Integer.valueOf(key))
                        case ScalaType.Long => PLong(java.lang.Long.valueOf(key))
                        case ScalaType.String => PString(key)
                        case _ => throw new RuntimeException(s"Unsupported type for key for ${fd.name}")
                      }
                      val msg = PMessage(
                        Map(keyDescriptor -> keyObj,
                          valueDescriptor -> parseSingleValue(cmp.messageCompanionForFieldNumber(fd.number), valueDescriptor, reader)))
                      vec :+= msg
                    } // end while
                    reader.readEndDocument()
                    Some((fd, PRepeated(vec)))
                  case _ => throw new BsonFormatException(
                    s"Expected an object for map field ${serializedName(fd)} of ${fd.containingMessage.name}")
                }
              } else if (fd.isRepeated) {
                reader.getCurrentBsonType match {
                  case BsonType.ARRAY =>
                    reader.readStartArray()
                    var vec = Vector[PValue]()
                    while (reader.readBsonType != BsonType.END_OF_DOCUMENT) {
                      vec :+= parseSingleValue(cmp, fd, reader)
                    }
                    reader.readEndArray()
                    Some((fd, PRepeated(vec)))
                  case _ =>
                    throw new BsonFormatException(
                      s"Expected an array for repeated field ${serializedName(fd)} of ${fd.containingMessage.name}")
                }
              } else {
                Some((fd, parseSingleValue(cmp, fd, reader)))
              }
            case None =>
              // need to skip value here
              reader.skipValue()
              None
          }
        } // end parseValue

        reader.getCurrentBsonType() match {
          case BsonType.DOCUMENT =>
            reader.readStartDocument()
            val maps: Map[String, FieldDescriptor] = cmp.scalaDescriptor.fields.map(fd => serializedName(fd) -> fd).toMap
            var valueMap: Map[FieldDescriptor, PValue] = Map()
            while(reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
              parseValue(maps, reader).foreach { tup =>
                valueMap += tup
              }
            }
            reader.readEndDocument()
            PMessage(valueMap)
          case t =>
            throw new BsonFormatException(s"Expected an object, found $t")
        }
      }

      @inline
      private def serializedName(fd: FieldDescriptor): String = {
        if (preservingProtoFieldNames) fd.asProto.getName else JsonFormat.jsonName(fd)
      }

      private def _encode(writer: BsonWriter, m: GeneratedMessage): Unit = {
        val descriptor = m.companion.scalaDescriptor
        writer.writeStartDocument()
        descriptor.fields.foreach { f =>
          val name = if (preservingProtoFieldNames) f.name else JsonFormat.jsonName(f)
          if (f.protoType.isTypeMessage) {
            serializeMessageField(f, name, m.getFieldByNumber(f.number), writer)
          } else {
            serializeNonMessageField(f, name, m.getField(f), writer)
          }
        }
        writer.writeEndDocument()
      }

      // serializeMessageField
      private def serializeMessageField(
        fd: FieldDescriptor,
        name: String,
        value: Any,
        writer: BsonWriter): Unit = {
        value match {
          case null =>
            // ignore
          case Nil =>
            if (includingDefaultValueFields) {
              if (fd.isMapField) {
                writer.writeStartDocument(name)
                writer.writeEndDocument()
              } else {
                writer.writeStartArray(name)
                writer.writeEndArray()
              }
            }
          case xs: Seq[GeneratedMessage] @unchecked =>
            if (fd.isMapField) {
              val mapEntryDescriptor = fd.scalaType.asInstanceOf[ScalaType.Message].descriptor
              val keyDescriptor = mapEntryDescriptor.findFieldByNumber(1).get
              val valueDescriptor = mapEntryDescriptor.findFieldByNumber(2).get
              writer.writeStartDocument(name)
              xs.foreach { x =>
                val key = x.getField(keyDescriptor) match {
                  case PBoolean(v) => v.toString
                  case PDouble(v) => v.toString
                  case PFloat(v) => v.toString
                  case PInt(v) => v.toString
                  case PLong(v) => v.toString
                  case PString(v) => v
                  case v => throw new BsonFormatException(s"Unexpected value for key: $v")
                }
                writer.writeName(key)
                if (valueDescriptor.protoType.isTypeMessage) {
                  val value = x.getFieldByNumber(valueDescriptor.number).asInstanceOf[GeneratedMessage]
                  _encode(writer, value)
                } else {
                  serializeSingleValue(writer, valueDescriptor, x.getField(valueDescriptor), formattingLongAsNumber)
                }
              }
              writer.writeEndDocument()
            } else {
              writer.writeStartArray(name)
              xs.foreach { msg =>
                _encode(writer, msg)
              }
              writer.writeEndArray()
            }
          case msg: GeneratedMessage =>
            writer.writeName(name)
            _encode(writer, msg)
          case v =>
            throw new BsonFormatException(v.toString)
        }
      }

      private def serializeSingleValue(writer: BsonWriter, fd: FieldDescriptor, value: PValue, formattingLongAsNumber: Boolean): Unit = value match {
        case PEnum(e) => writer.writeString(e.name) // we omit custom writer
        case PInt(v) => writer.writeInt32(v)
        case PLong(v) => if (formattingLongAsNumber) writer.writeInt64(v) else writer.writeString(v.toString)
        case PDouble(v) => writer.writeDouble(v)
        case PFloat(v) => writer.writeDouble(v)
        case PBoolean(v) => writer.writeBoolean(v)
        case PString(v) => writer.writeString(v)
        case PByteString(v) => writer.writeBinaryData(BsonBinary(v.toByteArray))
        case _: PMessage | PRepeated(_) | PEmpty => throw new RuntimeException("Should not happen!")
      }

      private def serializeDefaultValue(writer: BsonWriter, fd: FieldDescriptor) = {
        serializeSingleValue(writer, fd, JsonFormat.defaultValue(fd), formattingLongAsNumber)
      }

      private def serializeNonMessageField(fd: FieldDescriptor, name: String, value: PValue, writer: BsonWriter) = value match {
        case PEmpty =>
          if (includingDefaultValueFields) {
            writer.writeName(name)
            serializeDefaultValue(writer, fd)
          }
        case PRepeated(xs) =>
          if (xs.nonEmpty || includingDefaultValueFields) {
            writer.writeStartArray(name)
            xs.foreach { pv =>
              serializeSingleValue(writer, fd, pv, formattingLongAsNumber)
            }
            writer.writeEndArray()
          }
        case v =>
          if (includingDefaultValueFields ||
            !fd.isOptional ||
            !fd.file.isProto3 ||
            (v != JsonFormat.defaultValue(fd))) {
            writer.writeName(name)
            serializeSingleValue(writer, fd, v, formattingLongAsNumber)
          }
      }
    }
  }
}

object PBCodecBuilder {
  val DEFAULT_BUILDER = new PBCodecBuilder()
}
