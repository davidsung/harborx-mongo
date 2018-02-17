package com.harborx.mongo

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers, OptionValues}
import org.mongodb.scala._

abstract class MongoSpecBase
  extends FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with OptionValues {

  protected val db = MongoDAO.mongoClient.getDatabase("test")

}

object MongoDAO {
  val mongoClient: MongoClient = MongoClient("mongodb://127.0.0.1:27017/")
}
