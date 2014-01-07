package edu.chop.cbmi

import akka.kernel.Bootable
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import akka.routing.RoundRobinRouter
import scala.language.postfixOps
import scala.compat.Platform
import akka.event.Logging
import com.typesafe.config.Config
import edu.chop.cbmi.notifications.Email
import edu.chop.cbmi.util.{ConfigUtil, EC2, S3}
import java.io.File

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 4/5/13
 * Time: 9:43 AM
 */

/**
 * manages the actors that
 */

class GenesisApplication extends Bootable{
  val config = ConfigUtil.applicationConfig("genesis")
  val system = ActorSystem("GenesisApplication", config)

  //setup up the jobManager router
  val jobManagerConfigs = config.getConfigList("jobManagerService.jobManagers")
  val jobManagers = jobManagerConfigs.toArray.map{conf =>
    val ip = conf.asInstanceOf[Config].getString("ip")
    val port = conf.asInstanceOf[Config].getInt("port")
    system.actorFor(s"akka://JobManagerApplication@${ip}:${port}/user/jobManager")
  }

  val numJobManagers = jobManagers.length
  val jobRouter = system.actorOf(Props().withRouter(RoundRobinRouter(routees = jobManagers)))

  //setup job monitor actor
  val jobMonitor = system.actorOf(Props(new JobMonitorActor(jobRouter, numJobManagers)), "jobMonitor")

  def kJob(k:Int, N: Int, genes: GeneList, simFunc: SimFuncId, hpoTerms: HPOTermSet) = {
    jobMonitor ! KJob(k,N, genes, simFunc, jobMonitor, hpoTerms)
  }

  def shutDownOnComplete(force:Boolean = false) = jobMonitor ! ApplicationShutdown(this)

  def startup() {}

  def shutdown(){
    system.shutdown()
  }

}

class JobMonitorActor(jobRouter: ActorRef, numJobManagers: Int) extends Actor{

  lazy val runningJobs = scala.collection.mutable.ListBuffer[Job]()
  lazy val queuedJobs = scala.collection.mutable.ListBuffer[Job]()
  val actorStartTime = Platform.currentTime
  lazy val startTimes = scala.collection.mutable.Map[Job, Long]()
  val log = Logging(context.system, this)

  override def preStart() = {
    log.debug("Starting JobMonitorActor")
  }

  def startQueuedJob : Unit = {
    if(!queuedJobs.isEmpty){
      val nextJob = queuedJobs(0)
      queuedJobs -= nextJob
      self ! nextJob
    }
  }

  def receive = {
    case j:JobStatus => j match{
      case JobComplete(job) => {
        runningJobs -= job
        val now = Platform.currentTime
        val runTime = startTimes.get(job) match{
          case Some(start) => (now-start)/1000.
          case _ => -1
        }
        startTimes -= job
        log.info(s"Completed Job: ${job} at time $now, job run time: $runTime")
        startQueuedJob
      }
      case JobFailed(job) => {
        log.error(s"Job Failed: ${job}")
        //TODO job error handling or logging
      }
    }//end case JobStatus

    case job:Job => job match{

      case appShutdown:ApplicationShutdown => {
        if((runningJobs.length==0 && queuedJobs.length==0) || appShutdown.force){
          log.warning(s"Sending Shutdown To Remote Job Managers")
          if(S3.use_S3_?)S3.putFileInBucket(new File("./logs/akka.log"), s"akka_${Platform.currentTime.toString}.log")
          log.warning(s"Application Shutdown. Total Actor Up Time: ${(Platform.currentTime-actorStartTime)/1000.}")
          if(Email.notify_?)Email.sendEmail("Genesis Shutdown Notification", s"All Genesis jobs completed.\nTotal Actor Up Time: ${(Platform.currentTime-actorStartTime)/1000.}")
          Range(0,numJobManagers).foreach(i => jobRouter ! RemoteApplicationShutdown(true))
          if(EC2.terminate_instances_?){
            log.warning("Attempting to terminate all AWS EC2 instances")
            EC2.terminateAllInstances()
          }
          appShutdown.application.shutdown()
        }
        else {
          log.info(s"Received non-forced shutdown with ${queuedJobs.length} jobs in queue and ${runningJobs.length} running jobs at ${Platform.currentTime}")
          queuedJobs += appShutdown
        }
      }
      case _ => if(runningJobs.length<numJobManagers) {
        runningJobs += job
        val jobStartTime = Platform.currentTime
        jobRouter ! job
        startTimes += job->jobStartTime
        log.info(s"Starting Job: ${job} at time ${jobStartTime}")
      }else {
        log.info(s"Queued job: ${job} at ${Platform.currentTime}")
        queuedJobs += job
      }

    }//end case Job

    case err: Error => err match{
      case UnhandledJobType(job) => log.warning(s"Unhandled job type: ${job}")
      case UnhandledMessageType(msg) => log.warning(s"Unhandled message type: ${msg}")
      case _ => self ! UnhandledMessageType(err)
    }

    case msg: Any => sender ! UnhandledMessageType(s"Unknown msg type: ${msg}")

  }//end receiver

}//end JobMonitorActor




/**
 * main class
 * starts the manager application
 */
object Genesis {
  def run(N: Int, kvals: Set[Int], sfids: Set[SimFuncId]){
    println("Genesis application started")
    val app = new GenesisApplication

    kvals.foreach{k=>
      sfids.foreach{sfid =>
        val genes = AllAnnotatedEntrezGenes
        val hpoTerms = AnnotatingHPOTerms
        app.kJob(k, N, genes, sfid, hpoTerms)
      }
    }

    app.shutDownOnComplete()
  }
}