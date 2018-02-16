package com.harborx.mongo

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{TestKit, TestKitBase}
import com.harborx.mongo.implicits._
import org.bson.codecs.configuration.CodecRegistries.fromRegistries
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest.time.{Seconds, Span}

class MongoObservable2AkkaStreamSpec
  extends MongoSpecBase with TestKitBase {

  override implicit val system: ActorSystem = ActorSystem("MongoObservable2AkkaStreamSpec")

  implicit val futureConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(2, Seconds)))

  val codecRegistry = fromRegistries(DEFAULT_CODEC_REGISTRY)
  val collection = db.withCodecRegistry(codecRegistry).getCollection("mongo_sdk_akka_stream")

  override def beforeAll(): Unit = {
    collection.drop().toFuture().futureValue
  }

  override def afterAll(): Unit = {
    collection.drop().toFuture().futureValue
    TestKit.shutdownActorSystem(system)
  }

  "MongoDB Observable" should "covert to akka stream" in {
    collection.insertMany((1 to 10).map(i => Document("i" -> i))).toFuture().futureValue

    implicit val materializer = ActorMaterializer()

    Source.fromPublisher(collection.find[Document]())
      .map { _.getInteger("i") }
      .fold(0){ (total, i) => total + i }
      .runWith(Sink.seq).futureValue shouldBe Seq(55)
  }

}
