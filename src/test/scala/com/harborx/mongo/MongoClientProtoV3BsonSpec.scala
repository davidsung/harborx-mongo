package com.harborx.mongo

import com.harborx.mongo.codec.PBCodecBuilder
import com.harborx.mongo.test.v3.{MyEnumV3, MyTestV3}
import org.bson.codecs.configuration.CodecRegistries.{fromCodecs, fromRegistries}
import org.mongodb.scala.Completed
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.model.Filters
import org.scalatest.time.{Seconds, Span}

/**
  * V3 protobuf using custom codec
  */
class MongoClientProtoV3BsonSpec extends MongoSpecBase {

  // fake data
  private val TEST_PROTO_V3 = MyTestV3().update(
    _.hello := "Foo",
    _.foobar := 37,
    _.bazinga := 1481520538344L,
    _.primitiveSequence := Seq("a", "b", "c"),
    _.repMessage := Seq(MyTestV3(), MyTestV3(hello = "h11")),
    _.optMessage := MyTestV3().update(_.foobar := 39),
    _.stringToInt32 := Map("foo" -> 14, "bar" -> 19),
    _.intToMytest := Map(1 -> MyTestV3().update(
      _.hello := "Foo",
      _.foobar := 0,
      _.primitiveSequence := Seq(),
      _.stringToInt32 := Map()
    ).withTreat(MyTestV3().update(
      _.fixed64ToBytes := Map(1481520538344L -> com.google.protobuf.ByteString.copyFromUtf8("adsasdsadsad"))
    ).withTreat(MyTestV3().withHello("haha")))),
    _.repEnum := Seq(MyEnumV3.V1, MyEnumV3.V2, MyEnumV3.UNKNOWN),
    _.optEnum := MyEnumV3.V2,
    _.stringToBool := Map("ff" -> false, "tt" -> true),
    _.optBs := com.google.protobuf.ByteString.copyFromUtf8("abcdefghijklmnopqrstuvwxyz"),
    _.optBool := false,
    _.optDouble := 1.123,
    _.optFloat := 1.456.toFloat
  ).withTrick(32)

  implicit val futureConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(2, Seconds)))

  def test(builder: PBCodecBuilder) = {
    val reg = fromCodecs(builder.getCodecFor(classOf[MyTestV3]))
    val codecRegistry = fromRegistries(reg, DEFAULT_CODEC_REGISTRY)
    val collection = db.withCodecRegistry(codecRegistry).getCollection[MyTestV3]("mongo_sdk_proto_test_v3_codec")
    // before
    collection.drop().toFuture().futureValue
    // create
    collection.insertOne(TEST_PROTO_V3).toFuture().futureValue shouldBe Completed()
    // find
    collection.find(Filters.equal("hello", TEST_PROTO_V3.hello))
      .first()
      .toFuture()
      .futureValue shouldBe TEST_PROTO_V3
    // after
    collection.drop().toFuture().futureValue
  }

  "MongoDB protobuf read/write using PBCodecBuilder" should "work for test message" in {
    test(new PBCodecBuilder())
  }

  it should "work for formattingLongAsNumber=false" in {
    test(new PBCodecBuilder(formattingLongAsNumber = false))
  }

  it should "work for preservingProtoFieldNames=true" in {
    test(new PBCodecBuilder(preservingProtoFieldNames = true))
  }
}
