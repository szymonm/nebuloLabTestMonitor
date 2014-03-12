package org.nebulostore

import scala.collection.Seq

/**
 * Created by szymonmatejczyk on 21.01.2014.
 */
package object nebuloLabTestMonitor {

  sealed trait HostStatus
  case object Running extends HostStatus
  case object Up extends HostStatus
  case object Down extends HostStatus

  trait Message
  case class StartNetwork(val buildPath : String,
                          val hosts : Seq[(String, Int)]) extends Message
}
