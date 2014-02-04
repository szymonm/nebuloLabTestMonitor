package org.nebulostore.nebuloLabTestMonitor

import akka.actor._
import akka.event.Logging
import scala.sys.process._
import scala.concurrent._
import scala.collection._
import scala.concurrent.duration._
import scala.language.postfixOps
import ExecutionContext.Implicits.global
import rx.lang.scala._
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.NewThreadScheduler
import scala.util.Success
import scala.util.Failure
import rx.lang.scala.schedulers.ExecutorScheduler
import rx.lang.scala.schedulers.ThreadPoolForIOScheduler
import org.nebulostore.nebuloLabTestMonitor.utils.ObservableEx
import scala.util.Failure

/**
 * Created by szymonmatejczyk on 20.01.2014.
 */

class Controller(val username : String, path : String) extends Actor with ActorLogging {

  val KILL_CMD = "killall Nebulostore"
  def RUN_CMD(path : String) = s"ls $path"
  
  def COPY_LIB_CMD = "rsync -r target/lib sm262956@students.mimuw.edu.pl:/home/alumni/s/sm262956/nebulostore"
  def COPY_JAR_CMD = "rsync -r build/jar/1 sm262956@students.mimuw.edu.pl:/home/alumni/s/sm262956/nebulostore"
  def COPY_CONF_CMD = "rsync -r build/jar/1/conf sm262956@students.mimuw.edu.pl:/home/alumni/s/sm262956/nebulostore"
    
  def GREP_CMD = "ps au | grep Nebulostore | grep -v grep"

  val runningHosts = mutable.Set[String]()
  
  def sshCommand(username : String, host : String, path : String, cmd : String) = Seq(
    "ssh", "-f", "-o ConnectTimeout=1", s"$username@$host", cmd)

  def startHostCmd(host : String) =
    sshCommand(username, host, path, RUN_CMD(path))

  def startNebulo(host : String) : Future[Iterable[String]] = {
    val p = Promise[Iterable[String]]
    future {
      val output = mutable.Queue[String]()
      val processLogger = ProcessLogger(line => output.enqueue(line))
      startHostCmd(host).! (processLogger) match {
        case 0 => p.success(output)
        case _ => p.failure(new Exception(output.foldLeft("")(_+_)))
      }
    }
    p.future
  }


  def pingHost(host : String) : Future[Int] = {
    future {
      sshCommand(username, host, path, "ls") ! (ProcessLogger(x => ()))
    }
  }

  def monitorHost(host : String)(implicit execContext: ExecutionContext) : Observable[(Long, HostStatus)] = {
    Observable.interval(1.seconds).flatMap {
      x => ObservableEx({
        val startTime = System.currentTimeMillis()
        pingHost(host).map (
          i => if (i==0) ((startTime, Up)) else ((startTime, Down))
        ).recover{case t : Throwable => ((startTime, Down))}
      })
    }
  }

  def generateConfigs(hosts: Seq[String]) {}

  type Measurment = ((String, Long, HostStatus))
  def monitorHosts(hosts: Seq[String]) : Observable[Measurment] = {
    Observable.from[String](hosts).flatMap(host => monitorHost(host).map{case (time, status) => (host, time, status)})
  }

  def killNebulo(hosts : Seq[String]) {
    hosts.foreach{
      host => future {
        sshCommand(username, host, path, KILL_CMD).!
      }.recover{
        case e : Throwable => log.warning(s"Killing failed ${e.toString}")
      }
    }
  }

  def restartHosts(hosts : Seq[String]) {}

  def receive: Actor.Receive = {
    case StartNetwork(hosts) =>
      generateConfigs(hosts)
      hosts.foreach{
        host => startNebulo(host).onComplete{
          case Success(s) => log.debug(s"$host started")
                             runningHosts += host
          case Failure(t) => log.warning(s"$host failed to start")
        }
      }
      monitorHosts(hosts).observeOn(ThreadPoolForIOScheduler()).subscribe(_ match {
        case ((host, time, Up)) if !runningHosts.contains(host) =>
          log.debug(s"Starting Nebulo on $host")
          startNebulo(host).onComplete{
            case Success(_) => log.debug(s"Successfully started Nebulo on $host")
                               runningHosts += host
            case Failure(t) => log.warning(s"Failed to start Nebulo on $host: ${t.getMessage()}")
          }
        case ((host, time, Down)) if runningHosts.contains(host) =>
          log.debug(s"Host stopped: $host")
          runningHosts -= host
        case _ => ()
      }
      )
  }
}

object Main extends App {
  val actorSystem = ActorSystem("nebuloLabTestMonitor")
  val controller = actorSystem.actorOf(Props(classOf[Controller], "s.matejczyk", ""), "controller")

  controller ! StartNetwork(Seq("saturn.phd.ipipan.waw.pl", "ss.sl"))
}
