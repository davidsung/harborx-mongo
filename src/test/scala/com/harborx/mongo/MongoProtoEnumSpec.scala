package com.harborx.mongo

import com.harborx.mongo.test.v2.MyEnumV2
import com.harborx.mongo.test.v3.MyEnumV3
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonType, BsonWriter}
import org.mongodb.scala.Completed
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Filters
import org.scalatest.time.{Seconds, Span}

class MongoProtoEnumSpec extends MongoSpecBase {

  implicit val futureConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(2, Seconds)))
  private case class Home(address: String)
  private case class Person(name: String, age: Int, time: Option[Long], enumV3: MyEnumV3, enumV2: MyEnumV2, home: Home)

  private class TestPersonCodecProvider() extends CodecProvider {
    override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
      if (clazz == classOf[Person]) {
        new Codec[Person] {

          private val NAME = "name"
          private val AGE = "age"
          private val TIME = "time"
          private val ENUM_V3 = "enumV3"
          private val ENUM_V2 = "enumV2"
          private val HOME = "home"

          override def encode(writer: BsonWriter, value: Person, encoderContext: EncoderContext): Unit = {
            writer.writeStartDocument()
            writer.writeString(NAME, value.name)
            writer.writeInt32(AGE, value.age)
            value.time.foreach(time => writer.writeInt64(TIME, time))
            writer.writeString(ENUM_V3, value.enumV3.name)
            writer.writeString(ENUM_V2, value.enumV2.name)
            writer.writeName(HOME)
            registry.get(classOf[Home]).encode(writer, value.home, encoderContext)
            writer.writeEndDocument()
          }

          override def getEncoderClass: Class[Person] = classOf[Person]

          override def decode(reader: BsonReader, decoderContext: DecoderContext): Person = {
            reader.readStartDocument()
            var name: Option[String] = None
            var age: Option[Int] = None
            var time: Option[Long] = None
            var enumV3: Option[MyEnumV3] = None
            var enumV2: Option[MyEnumV2] = None
            var home: Option[Home] = None
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
              reader.readName() match {
                case NAME =>
                  name = Option(reader.readString())
                case AGE =>
                  age = Option(reader.readInt32())
                case TIME =>
                  time = Option(reader.readInt64())
                case ENUM_V3 =>
                  enumV3 = MyEnumV3.fromName(reader.readString())
                case ENUM_V2 =>
                  enumV2 = MyEnumV2.fromName(reader.readString())
                case HOME =>
                  home = Option(registry.get(classOf[Home]).decode(reader, decoderContext))
                case _ =>
                  reader.skipValue()
              }
            }
            reader.readEndDocument()
            if (Seq(name, age, enumV2, enumV3, home).contains(None)) {
              throw new IllegalStateException("some null...")
            }
            Person(
              name = name.get,
              age = age.get,
              time = time,
              enumV3 = enumV3.get,
              enumV2 = enumV2.get,
              home = home.get
            )
          }
        }.asInstanceOf[Codec[T]]
      } else {
        null
      }
    }
  }

  // fake data
  private val TEST_DATA = Person(
    name = "hehe",
    age = 1,
    time = Some(0L),
    enumV3 = MyEnumV3.V1,
    enumV2 = MyEnumV2.V2,
    home = Home("address")
  )
  private val codecRegistry = fromRegistries(fromProviders(new TestPersonCodecProvider), fromProviders(classOf[Home]),  DEFAULT_CODEC_REGISTRY)
  private val collection = db.withCodecRegistry(codecRegistry).getCollection[Person]("mongo_sdk_proto_enum_codec_test")

  override def beforeAll(): Unit = {
    collection.drop().toFuture().futureValue
  }

  override def afterAll(): Unit = {
    collection.drop().toFuture().futureValue
  }

  "case object enum" should "encode/decode with custom Codec[A]" in {
    collection.insertOne(TEST_DATA).toFuture().futureValue shouldBe Completed()
    collection.find(Filters.equal("name", TEST_DATA.name))
      .first()
      .toFuture()
      .futureValue shouldBe TEST_DATA
  }

}
