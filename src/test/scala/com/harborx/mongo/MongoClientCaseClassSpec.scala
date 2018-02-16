package com.harborx.mongo

import java.util.UUID

import com.harborx.mongo.codec.UUIDStringCodec
import com.harborx.mongo.exception.DataNotFoundException
import com.harborx.mongo.implicits._
import com.mongodb.client.model.ReturnDocument
import org.bson.codecs.configuration.CodecRegistries.{fromCodecs, fromProviders, fromRegistries}
import org.mongodb.scala.Completed
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.{Filters, FindOneAndUpdateOptions}
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * normal case class case
  */
class MongoClientCaseClassSpec extends MongoSpecBase {

  implicit val futureConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(2, Seconds)))

  // some fake case class that simulate
  private case class Person(name: String, age: Int, time: Long, optBoolean: Option[Boolean], optDouble: Option[Double] = None)
  private case class Home(name: String, members: List[Person], master: Person, uuid: UUID)

  private val codecRegistry = fromRegistries(fromCodecs(new UUIDStringCodec), fromProviders(classOf[Person], classOf[Home]), DEFAULT_CODEC_REGISTRY)

  // need explicit type
  private val personCollection = db.withCodecRegistry(codecRegistry).getCollection[Person]("mongo_sdk_case_class_test_person")
  private val homeCollection = db.withCodecRegistry(codecRegistry).getCollection[Home]("mongo_sdk_case_class_test_home")

  override def beforeAll(): Unit = {
    personCollection.drop().toFuture().futureValue
    homeCollection.drop().toFuture().futureValue
  }

  override def afterAll(): Unit = {
    personCollection.drop().toFuture().futureValue
    homeCollection.drop().toFuture().futureValue
  }

  "mongodb single case class CURD" should "work" in {
    val person = Person("Ada", 18, 1504772670000L, Some(true))
    // create
    personCollection.insertOne(person).toFuture().futureValue shouldBe Completed()
    // read
    personCollection.find(Filters.equal("name", person.name)).first().toFuture().futureValue shouldBe person
    // update
    import org.mongodb.scala.model.Updates._
    val option = FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
    personCollection.findOneAndUpdate(Filters.equal("name", person.name), set("age", 110), option).toFuture().futureValue shouldBe person.copy(age = 110)
    // delete
    personCollection.deleteOne(Filters.equal("name", person.name)).toFuture().futureValue.getDeletedCount shouldBe 1L
    // get again
    personCollection.find(Filters.equal("name", person.name)).toFuture().futureValue shouldBe Seq()
    personCollection.find(Filters.equal("name", person.name)).first().toFuture().map(Option.apply).futureValue shouldBe None // method 1
    personCollection.find(Filters.equal("name", person.name)).first().toFutureOption().futureValue shouldBe None // method 2
    personCollection.find(Filters.equal("name", person.name)).first().toSafeFuture().failed.futureValue shouldBe an[DataNotFoundException] // method 3

  }

  "mongodb nested case class" should "work" in {
    val person = Person("Ada", 18, 1504772670000L, Some(true))
    val home = Home(name = "name", members = List(person), master = person, uuid = UUID.randomUUID())
    homeCollection.insertOne(home).toFuture().futureValue shouldBe Completed()
    homeCollection.find(Filters.equal("name", "name")).first().toSafeFuture().futureValue shouldBe home
  }

}
