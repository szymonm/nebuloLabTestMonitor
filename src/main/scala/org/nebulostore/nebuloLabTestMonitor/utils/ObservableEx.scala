package org.nebulostore.nebuloLabTestMonitor.utils

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import rx.lang.scala.Observable
import rx.lang.scala.subjects.ReplaySubject
import scala.util.Success
import scala.util.Failure

object ObservableEx {

  /**
   * Returns an observable stream of values produced by the given future.
   * If the future fails, the observable will fail as well.
   *
   * @param f future whose values end up in the resulting observable
   * @return an observable completed after producing the value of the future, or with an exception
   */
  def apply[T](f: Future[T])(implicit execContext: ExecutionContext): Observable[T] = {
    val rs = ReplaySubject[T]()
    f.onComplete {
      case Success(s) => rs.onNext(s)
    		  			 rs.onCompleted()
      case Failure(t) => rs.onError(t)
    }
    rs
  }

}