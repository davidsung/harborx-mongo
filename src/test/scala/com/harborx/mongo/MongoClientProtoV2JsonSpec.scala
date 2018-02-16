package com.harborx.mongo

import com.harborx.mongo.test.v2.{MyEnumV2, MyTestV2}
import com.trueaccord.scalapb.json.JsonFormat
import org.bson.codecs.configuration.CodecRegistries.fromRegistries
import org.mongodb.scala.Completed
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters
import org.scalatest.time.{Seconds, Span}

/**
  * using scalapb JsonFormat
  */
class MongoClientProtoV2JsonSpec extends MongoSpecBase {

  implicit val futureConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(2, Seconds)))

  private val codecRegistry = fromRegistries(DEFAULT_CODEC_REGISTRY)

  private val collection = db.withCodecRegistry(codecRegistry).getCollection("mongo_sdk_proto_test_v2")

  override def beforeAll(): Unit = {
    collection.drop().toFuture().futureValue
  }

  override def afterAll(): Unit = {
    collection.drop().toFuture().futureValue
  }

  "MongoDB protobuf read/write using JsonFormat" should "work for test message" in {
    // fake data
    val testProto = MyTestV2().update(
      _.hello := "Foo",
      _.foobar := 37,
      _.bazinga := 1481520538344L,
      _.primitiveSequence := Seq("a", "b", "c"),
      _.repMessage := Seq(MyTestV2(), MyTestV2(hello = Some("h11"))),
      _.optMessage := MyTestV2().update(_.foobar := 39),
      _.stringToInt32 := Map("foo" -> 14, "bar" -> 19),
      _.repEnum := Seq(MyEnumV2.V1, MyEnumV2.V2, MyEnumV2.UNKNOWN),
      _.optEnum := MyEnumV2.V2,
      _.stringToBool := Map("ff" -> false, "tt" -> true),
      _.optBool := false
    ).withTrick(32)
    // create
    collection.insertOne(Document(JsonFormat.toJsonString(testProto))).toFuture().futureValue shouldBe Completed()
    // find
    import scala.concurrent.ExecutionContext.Implicits.global
    collection.find(Filters.equal("hello", testProto.getHello))
      .first()
      .toFuture()
      .map(doc => JsonFormat.fromJsonString[MyTestV2](doc.toJson()))
      .futureValue shouldBe testProto
  }
}
