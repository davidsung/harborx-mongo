package com.harborx.mongo

import com.harborx.mongo.codec.PBCodecBuilder
import com.harborx.mongo.test.v2.{MyEnumV2, MyTestV2}
import org.bson.codecs.configuration.CodecRegistries.{fromCodecs, fromRegistries}
import org.mongodb.scala.Completed
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.model.Filters
import org.scalatest.time.{Seconds, Span}

/**
  * V2 protobuf using custom codec
  */
class MongoClientProtoV2BsonSpec extends MongoSpecBase {

  // fake data
  private val TEST_PROTO_V2 = MyTestV2().update(
    _.hello := "Foo",
    _.foobar := 37,
    _.bazinga := 1481520538344L,
    _.primitiveSequence := Seq("a", "b", "c"),
    _.repMessage := Seq(MyTestV2(), MyTestV2(hello = Some("h11"))),
    _.optMessage := MyTestV2().update(_.foobar := 39),
    _.stringToInt32 := Map("foo" -> 14, "bar" -> 19),
    _.intToMytest := Map(1 -> MyTestV2().update(
      _.hello := "Foo",
      _.foobar := 0,
      _.primitiveSequence := Seq(),
      _.stringToInt32 := Map()
    ).withTreat(MyTestV2().update(
      _.fixed64ToBytes := Map(1481520538344L -> com.google.protobuf.ByteString.copyFromUtf8("adsasdsadsad"))
    ).withTreat(MyTestV2().withHello("haha")))),
    _.repEnum := Seq(MyEnumV2.V1, MyEnumV2.V2, MyEnumV2.UNKNOWN),
    _.optEnum := MyEnumV2.V2,
    _.stringToBool := Map("ff" -> false, "tt" -> true),
    _.optBs := com.google.protobuf.ByteString.copyFromUtf8("abcdefghijklmnopqrstuvwxyz"),
    _.optBool := false,
    _.optDouble := 1.123,
    _.optFloat := 1.456.toFloat
  ).withTrick(32)

  implicit val futureConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(2, Seconds)))

  def test(builder: PBCodecBuilder) = {
    val reg = fromCodecs(builder.getCodecFor(classOf[MyTestV2]))
    val codecRegistry = fromRegistries(reg, DEFAULT_CODEC_REGISTRY)
    val collection = db.withCodecRegistry(codecRegistry).getCollection[MyTestV2]("mongo_sdk_proto_test_v2_codec")
    // before
    collection.drop().toFuture().futureValue
    // create
    collection.insertOne(TEST_PROTO_V2).toFuture().futureValue shouldBe Completed()
    // find
    collection.find(Filters.equal("hello", TEST_PROTO_V2.getHello))
      .first()
      .toFuture()
      .futureValue shouldBe TEST_PROTO_V2
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
