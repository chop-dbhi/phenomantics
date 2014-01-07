package edu.chop.cbmi

import akka.actor.{Props, ActorSystem, Actor, ActorRef}
import edu.chop.cbmi.phenomantics.api.ontologies.{HPOEntrezFactory, HPO_Term}
import scala.language.postfixOps
import akka.kernel.Bootable
import akka.routing.RoundRobinRouter
import akka.event.Logging
import com.typesafe.config.Config
import scala.compat.Platform
import scala.util.Random
import scala.annotation.tailrec
import edu.chop.cbmi.phenomantics.api.gene.EntrezGeneSet
import edu.chop.cbmi.util.ConfigUtil

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 4/10/13
 * Time: 12:42 PM
 */

object JobManagerApp{
  def run(){
    new JobManagerApplication
    println("Started JobManager Application")
  }
}

class JobManagerApplication extends Bootable{

  val config = ConfigUtil.applicationConfig("jobmanager")
  val system = ActorSystem("JobManagerApplication", config)

  //setup up the worker router
  val workersConfig = config.getConfigList("workerService.workers")
  val workers: Array[ActorRef] = workersConfig.toArray.map{conf=>
    val ip = conf.asInstanceOf[Config].getString("ip")
    val port = conf.asInstanceOf[Config].getInt("port")
    system.actorFor(s"akka://WorkerApplication@${ip}:${port}/user/router")
  }
  val workerRouter = system.actorOf(Props().withRouter(RoundRobinRouter(routees = workers)), "workerRouter")

  val storageActors : Array[ActorRef] = workersConfig.toArray.map{conf=>
    val ip = conf.asInstanceOf[Config].getString("ip")
    val port = conf.asInstanceOf[Config].getInt("storagePort")
    system.actorFor(s"akka://StorageApplication@${ip}:${port}/user/storageActor")
  }
  val storageRouter = system.actorOf(Props().withRouter(RoundRobinRouter(routees=storageActors)), "storageRouter")

  //setup job manager actor
  val jobManager = system.actorOf(Props(new JobManagerActor(workerRouter, workers.length, storageRouter)), "jobManager")

  def startup(){}

  def shutdown(){
    println("Shutting down Job Manager Application")
    system.shutdown()
  }
}

class JobManagerActor(workerRouter: ActorRef, numWorkers: Int, storageRouter: ActorRef) extends Actor {
  val log = Logging(context.system, this)
  lazy val ao = HPOEntrezFactory.informationAO
  val jobs = scala.collection.mutable.Map[Int, Job]()
  val rootStoragePath = ConfigUtil.applicationConfig("filestorage").getString("root")

  //mapping from job ID to number of simScore results received
  lazy val jobReceivedScoreResults = scala.collection.mutable.Map[Int,Int]()

  //mapping from job ID to job start time
  lazy val jobStartTimes = scala.collection.mutable.Map[Int, Long]()

  //mapping from job ID to job storages completed
  lazy val jobStorageCompleted = scala.collection.mutable.Map[Int, Int]()

  @tailrec
  final def addJob(job:Job) : Int = {
    val id : Int = rand.nextInt();
    if(jobs.keySet.contains(id))addJob(job)
    else{
      jobs += (id -> job)
      id
    }
  }

  def validate_n(n:Int, hpoTerms:HPOTermSet) = hpoTerms match{
    case AllHPOTerms => math.min(n, HPO_Term.allTerms.size)
    case AnnotatingHPOTerms => math.min(n, ao.annotatingConcepts.size)
    case _ => n
  }

  lazy val rand = new Random()
  def randomSet(k:Int, max: Int) = (Seq.fill(k))(rand.nextInt(max)).toSet

  def genRandomPhenotypeSet(k: Int, termSet: HPOTermSet) = {
    val terms = hpo(termSet)
    val maxHpoIndex = terms.length
    val rs = randomSet(k, maxHpoIndex)
    rs
  }

  def hpo(termSet: HPOTermSet) = {
    termSet match {
      case AllHPOTerms => Worker.hpoAll
      case AnnotatingHPOTerms => Worker.hpoAnnotating
      case _ => Array[HPO_Term]()
    }
  }

  @tailrec
  private def addRandomSet(s:Set[Set[Int]], k:Int, hpoTerms: HPOTermSet) : Set[Set[Int]]= {
    val ns = genRandomPhenotypeSet(k, hpoTerms)
    if(s.contains(ns))addRandomSet(s, k, hpoTerms)
    else s.+(ns)
  }

  def receive = {
    case (job: Job) => {
      val id : Int = addJob(job)
      job match{
        case KJob(k, n, genes, simFunc, jobMonitor, hpoTerms) =>{
          jobStartTimes(id)=Platform.currentTime
          val N = if(k==1)validate_n(n, hpoTerms) else n
          log.info(s"Starting KJob=${job}, N=${N}")
          val messageLength = 5000 / k
          val (samplesSent,remainingSamples) = ((0,Set[Set[Int]]()) /: Range(0,N)){(tpl,i) =>
            val s = tpl._2
            val t = tpl._1
            val samples = if(k==1) s.+(Set(i)) else addRandomSet(s,k,hpoTerms)
            if(samples.size % messageLength == 0){
              //send phenotypes to workers
              Range(0,numWorkers).foreach{i=>workerRouter ! AddPhenotypeIndicesForJob(samples, id)}
              log.info(s"Sent $messageLength new samples to workers, total num samples = ${t + messageLength}")
              (t + messageLength, Set[Set[Int]]())
            }else (t,samples)
          }
          val remainingCount = remainingSamples.size
          if(remainingCount >0){
            //send phenotypes to workers
            Range(0,numWorkers).foreach{i=>workerRouter ! AddPhenotypeIndicesForJob(remainingSamples, id)}
            log.info(s"Sent ${remainingCount} new samples to workers, total num samples = ${samplesSent + remainingCount}")
          }
          //send genes to workers
          genes match{
            case AllAnnotatedEntrezGenes => EntrezGeneSet.hpo_annotated_genes.foreach{gene =>
              workerRouter ! ComputeAndStoreSimScoresAllModels(gene, hpoTerms, id, k)
            }
            case SpecificGenes(geneIds) => geneIds.foreach{gid =>
              ao.entrezGeneById(gid) match{
                case Some(gene) => workerRouter ! ComputeAndStoreSimScoresAllModels(gene, hpoTerms, id, k)
                case _ => log.error(s"No Entrez Gene found for id: $gid")
              }
            }
          }
        }

        case RemoteApplicationShutdown(force) => {
          if(force){
            log.warning("Received Application Shutdown Message.")

            val count = ConfigUtil.applicationConfig("jobmanager").getConfigList("workerService.workers").size()
            Range(0,count).foreach{i=>
              storageRouter ! RemoteApplicationShutdown(force)
              workerRouter ! RemoteApplicationShutdown(force)
            }
            context.system.shutdown()
          }
        }

        case _ => {
          log.warning(s"Receieved unhandled job type: ${job}")
          jobs -= id
          sender ! UnhandledJobType(job)
        }
      } // end job match
    } //end case job

    case result: Result => jobs.get(result.jobID) match {
      case Some(job) => job match{

        case KJob(k,n,genes,simFunc,jobMonitor, hpoTerms) => {
          val G = genes match{
            case AllAnnotatedEntrezGenes => EntrezGeneSet.hpo_annotated_genes.size
            case SpecificGenes(genes) => genes.length
          }


          result match{

            case ComputeAndStoreSimScoresComplete(gene, jobID) => {
              val count = jobReceivedScoreResults.getOrElse(jobID, 0) + 1
              if(count == G) {
                val N = if(k==1)validate_n(n, hpoTerms) else n
                log.info(s"All scores completed for jobID=${jobID}, count=${count}, N=${N}")
                Range(0,numWorkers).foreach{i=> storageRouter ! TarAndS3SimScores(rootStoragePath, k, i, jobID)}
              }else{
                jobReceivedScoreResults(jobID)=count
                val l = G.toDouble
                val r = count/l
                val percent = r * 100
                val toc = Platform.currentTime
                jobStartTimes.get(jobID) match{
                  case Some(start) =>{
                    val totalTimeEst = (Platform.currentTime - start)/((1000 * 60 * 60).toDouble)/r
                    val eta = (1.0 - r)*totalTimeEst
                    log.info(s"Terms: $l, Computed: $count, Percent: ${count/l*100}, Total Time: ${(toc-start)/(1000.0 * 60.0 *60)} hours, TTE: $totalTimeEst hours, ETA:$eta hours")
                  }
                  case _ =>{
                    log.info(s"jobID:${jobID}, ${count} results of ${G}, ${percent}% complete")
                  }
                }
              }
            }

            case TarAndS3SimScoresComplete(storageID, jobID) => {
              val count = jobStorageCompleted.getOrElse(jobID, 0) + 1
              if(count == numWorkers){
                log.info(s"All storage reported completed, count=$count, jobID=$jobID")
                self ! JobManagerJobComplete(jobID)
              }else{
                jobStorageCompleted(jobID)=count
                log.info(s"Storage completed for $count of $numWorkers")
              }

            }

            case JobManagerJobComplete(jobID) => {
              jobs -= jobID
              jobReceivedScoreResults -= jobID
              jobStorageCompleted -= jobID
              log.info(s"Job completed, jobID=${jobID}")
              Range(0, numWorkers).foreach{i => workerRouter ! DropPhenotypeIndiciesForJob(jobID)}
              jobMonitor ! JobComplete(job)
            }
          }//end result match
        }//end case KJob

        //This is impossible unless I foolishly code the Worker actor to create Job instances
        case _ => self ! UnhandledJobType(job)

      }//end Some(job)

      //job not found for this result
      case _ => self ! UnhandledMessageType(s"Invalid job ID : ${result.jobID} for result: ${result}")

    }//end case result

    case err: Error => err match{
      case UnhandledOp(op) => log.warning(s"Unhandled Operation: ${op}")
      case UnhandledJobType(job) => log.warning(s"Unhandled Jot Type: ${job}")
      case UnhandledMessageType(msg) => log.warning(s"Unhandled Message: ${msg}")
      case UnhandledResultType(r) => log.warning(s"Unhandled Result Type: ${r}")
      case _ => self ! UnhandledMessageType(err)
    }

    //default message handler
    case msg: Any => sender ! UnhandledMessageType(s"Unknown msg type: ${msg}")

  }//end receive

}//end KJobManagerActor
