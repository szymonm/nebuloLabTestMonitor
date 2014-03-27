package org.nebulostore.nebuloLabTestMonitor

import akka.actor._
import scala.sys.process._
import scala.concurrent._
import scala.collection._
import scala.concurrent.duration._
import scala.language.postfixOps
import ExecutionContext.Implicits.global
import rx.lang.scala._

import scala.util.Success
import scala.util.Failure
import org.nebulostore.nebuloLabTestMonitor.utils.ObservableEx
import rx.lang.scala.schedulers.ThreadPoolForIOScheduler


/**
 * Created by szymonmatejczyk on 20.01.2014.
 */

class Controller(val username : String, nebuloPath : String) extends Actor with ActorLogging {

  def buildClientVersion(hostsNum : Int) : Int = {
    s"$nebuloPath/nebulostore/scripts/_build-and-deploy.sh -p $hostsNum -m peer" !
  }

  def prepareConfigurations(buildsPath : String, hosts : Seq[(String, Int)]) : Int = {
    hosts.map{
      host => s"$nebuloPath/nebulostore/scripts/prepare-lab-configuration.sh ${host._2}" !
    }.max
  }

  def prepareHosts(buildsPath : String, hosts : Seq[(String, Int)]) : Int = {
    buildClientVersion(hosts.size)
    prepareConfigurations(buildsPath, hosts)
    hosts.map{
      case (hostName, hostNum) =>
        s"rm -rf $nebuloPath/$hostName" #&& s"mkdir -p $nebuloPath/$hostName" #&&
          s"mv $buildsPath/$hostNum $nebuloPath/$hostName" !
    }.max
  }

  val KILL_CMD = "killall Nebulostore"
  def RUN_CMD(hostName : String) = s"cd $nebuloPath/$hostName && java -jar Nebulostore.jar"

  def PREPARE_DIRS_CMD(host : String) : String = {
    val path = nebuloPath
    def mkdir(dir : String) = s"mkdir -p $path$dir"
    def ln(from : String, to : String) = s"ln -s $to $from"
    List(mkdir(s"lib"), mkdir(s"$host/resources"), ln(nebuloPath + host + "/lib", nebuloPath + "/lib"))
      .mkString(" && ")
  }

  def GREP_CMD = "ps au | grep Nebulostore | grep -v grep"

  val runningHosts = mutable.Set[String]()
  
  def sshCommand(username : String, host : String,
                 path : String, cmd : String) = Seq(
    "ssh", "-f", "-o ConnectTimeout=1", s"$username@$host", cmd)

  def startHostCmd(host : String) =
    sshCommand(username, host, nebuloPath, RUN_CMD(host))

  def startNebulo(host : String) : Future[Iterable[String]] = {
    val p = Promise[Iterable[String]]()
    future {
      val output = mutable.Queue[String]()
      val processLogger = ProcessLogger(line => output.enqueue(line))
      startHostCmd(host) ! processLogger match {
        case 0 => p.success(output)
        case _ => p.failure(new Exception(output.foldLeft("")(_+_)))
      }
    }
    p.future
  }


  def pingHost(host : String) : Future[Int] = {
    future {
      sshCommand(username, host, nebuloPath, "ls") ! (ProcessLogger(x => ()))
    }
  }

  def checkNebulostore(host : String) : Future[Boolean] = {
    future {
      Process(sshCommand(username, host, "", GREP_CMD)).lines.nonEmpty
    }
  }

  def monitorHost(host : String)(implicit execContext: ExecutionContext)
      : Observable[(Long, HostStatus)] = {
    Observable.interval(1.seconds).flatMap {
      x => ObservableEx({
        val startTime = System.currentTimeMillis()
        checkNebulostore(host).map {
          case true => ((startTime, Running))
          case false => ((startTime, Up))
        }.recover{case t : Throwable => ((startTime, Down))}
      })
    }
  }

  def generateConfigs(hosts: Seq[String]) {}

  type Measurment = ((String, Long, HostStatus))
  def monitorHosts(hosts: Seq[String]) : Observable[Measurment] = {
    Observable.from[String](hosts).flatMap(host => monitorHost(host).map{
      case (time, status) => (host, time, status)})
  }

  def killNebulo(hosts : Seq[String]) {
    hosts.foreach{
      host => future {
        sshCommand(username, host, nebuloPath, KILL_CMD).!
      }.recover{
        case e : Throwable => //log.warning(s"Killing failed ${e.toString}")
      }
    }
  }

  def restartHosts(hosts : Seq[String]) {}

  def receive: Actor.Receive = {
    case StartNetwork(buildsPath, hostsList) =>
      prepareHosts(buildsPath, hostsList)
      val hosts = hostsList.map(_._1)
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
            case Failure(t) => log.warning(s"Failed to start Nebulo on " +
                                           s"$host: ${t.getMessage()}")
          }
        case ((host, time, Up)) if runningHosts.contains(host) =>
          runningHosts -= host
          log.debug(s"Starting Nebulo on $host")
          startNebulo(host).onComplete{
            case Success(_) => log.debug(s"Successfully started Nebulo on $host")
                               runningHosts += host
            case Failure(t) => log.warning(s"Failed to start Nebulo on $host:" +
                                           t.getMessage())
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
//
  val homeDir = System.getProperty("user.home")
  println(homeDir)
  val hosts = Seq("orange01", "orange02", "orange03").zipWithIndex
    .map{case (x, y) => (x, y + 1)}

  val controller = actorSystem.actorOf(Props(classOf[Controller], "sm262956",
    s"$homeDir/nebulo"), "controller")

  controller ! StartNetwork(s"$homeDir/nebulo/nebulostore/build/jar",
    hosts)
}
