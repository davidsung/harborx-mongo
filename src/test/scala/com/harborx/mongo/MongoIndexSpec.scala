package com.harborx.mongo

import com.harborx.mongo.index._
import org.mongodb.scala.model.IndexOptions
import org.mongodb.scala.model.Indexes._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * Test create indexes
  */
class MongoIndexSpec extends MongoSpecBase {

  val dbName = "mongo_sdk_index_test"
  val collection = db.getCollection(dbName)

  override def beforeAll(): Unit = {
    collection.drop().toFuture().futureValue
  }

  override def afterAll(): Unit = {
    collection.drop().toFuture().futureValue
  }

  "DBPopulator" should "executeIndexCreation" in {
    val indexConfig = List(
      MongoDBCollectionConfig(dbName, List(
        MongoDBIndexConfig(
          key = compoundIndex(ascending("quantity", "totalAmount"), descending("orderDate")),
          options = IndexOptions().background(true).unique(true)
        )
      ))
    )
    DBPopulator.executeIndexCreationBlocking(db, indexConfig, onFinish = name => {
      case Success(s) =>
        println(s"index [$s] created for [$name] success")
      case Failure(e) =>
        println(s"index for [$name] fail with error:${e.getMessage}")
    })
    collection.listIndexes().toFuture().futureValue.exists { doc =>
      doc.getString("name") == "quantity_1_totalAmount_1_orderDate_-1"
    } shouldBe true
  }

}
