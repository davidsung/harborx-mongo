package com.harborx.mongo.index

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.IndexOptions

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

case class MongoDBCollectionConfig(name: String, xs: List[MongoDBIndexConfig])
case class MongoDBIndexConfig(key: Bson, options: IndexOptions)

object DBPopulator {
  private val empty_pf : (String) => PartialFunction[Try[String], Unit] = _ => PartialFunction({ _ => () })
  def executeIndexCreation(db: MongoDatabase,
                           configs: List[MongoDBCollectionConfig],
                           onFinish: (String) => PartialFunction[Try[String], Unit] = empty_pf)
                          (implicit ec: ExecutionContext): Future[Seq[String]] = {
    Future.sequence(
      configs.flatMap { collectionConfig =>
        collectionConfig.xs.map { config =>
          db.getCollection(collectionConfig.name)
            .createIndex(config.key, config.options)
            .toFuture()
            .andThen(onFinish(collectionConfig.name))
        }
      }
    )
  }

  def executeIndexCreationBlocking(db: MongoDatabase,
                                   configs: List[MongoDBCollectionConfig],
                                   atMost: Duration = 5 seconds,
                                   onFinish: (String) => PartialFunction[Try[String], Unit] = empty_pf)
                                  (implicit ec: ExecutionContext): Seq[String] = {
    Await.result(executeIndexCreation(db, configs, onFinish), atMost)
  }
}
