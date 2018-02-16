package com.harborx.mongo.implicits

import com.harborx.mongo.exception.DataNotFoundException
import org.mongodb.scala.SingleObservable

import scala.concurrent.{ExecutionContext, Future}

trait SingleObservableImplicit {
  implicit class ScalaSingleObservableImplicit[T](observable: SingleObservable[T]) {
    def toFutureOption()(implicit ec: ExecutionContext): Future[Option[T]] = observable.head().map(Option.apply)

    /**
      * This Future is safe from NPE
      */
    def toSafeFuture()(implicit ec: ExecutionContext): Future[T] =
      observable.toFutureOption().flatMap(_.map(Future.successful).getOrElse(Future.failed(new DataNotFoundException)))
  }
}
