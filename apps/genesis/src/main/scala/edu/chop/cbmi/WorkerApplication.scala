package edu.chop.cbmi

import akka.actor._
import phenomantics.api.gene.{EntrezGene, EntrezGeneSet}
import phenomantics.api.ontologies.{HPO_Term, HPOEntrezFactory}
import akka.kernel.Bootable
import akka.routing.RoundRobinRouter
import scala.language.postfixOps
import akka.event.{LoggingAdapter, Logging}
import scala.compat.Platform
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import java.io.File
import edu.chop.cbmi.storage.{StorageApplication,FileStorage}
import edu.chop.cbmi.util.ConfigUtil

object Worker{

  lazy val log = LoggerFactory.getLogger(Worker.getClass)
  lazy val ao = HPOEntrezFactory.informationAO
  lazy val hpoAll: Array[HPO_Term] = sortedTerms(AllHPOTerms)
  lazy val hpoAnnotating: Array[HPO_Term] = sortedTerms(AnnotatingHPOTerms)
  lazy val hpoAnnotatedGeneIndices = mapGenesToHpoIndices(EntrezGeneSet.hpo_annotated_genes, hpoAnnotating)
  lazy val hpoAllGeneIndices = mapGenesToHpoIndices(EntrezGeneSet.hpo_annotated_genes, hpoAll)
  lazy val hpoAllPairWiseSims = Worker.computePairWiseSimilarties(AllHPOTerms)

  lazy val hpoAnnotatingPairWiseSims: ArrayBuffer[(Double, Double)] = {
    val config = ConfigFactory.parseFile(new File("./conf/application.conf")).getConfig("worker.hpoAnnotatingPairWiseSims")
    val useFie = config.getString("useFile").trim.toLowerCase == "true"
    if(useFie){
      log.info("Starting loading of hpoAnnotatingPairwiseSims from file")
      val path = config.getString("filePath")
      val tic = Platform.currentTime
      val a = PreComputeApplication.deserialize[ArrayBuffer[(Double,Double)]](path)
      val toc = Platform.currentTime
      log.info(s"Loading hpoAnnotatingPairWiseSims from file took: ${(toc-tic)/1000.0}")
      a
    } else Worker.computePairWiseSimilarties(AnnotatingHPOTerms)
  }

  def mapGenesToHpoIndices(genes:Set[EntrezGene], terms: Array[HPO_Term]): Map[EntrezGene, Set[Int]] = {
    (Map[EntrezGene, Set[Int]]() /: genes){(m, g) =>
      val s = ao.annotationsFor(g) match{
        case Some(annotations) => {
          annotations.map{a=>
            terms.indexOf(a.value)
          }
        }
        case _ => Set[Int]()
      }
      m.+((g,s))
    }
  }

  def sortedTerms(termSet: HPOTermSet) = {
    def termSorter(t1:HPO_Term, t2:HPO_Term) = t1.id<t2.id
    termSet match{
      case AllHPOTerms => HPO_Term.allTerms.toList.sortWith{termSorter}.toArray
      case AnnotatingHPOTerms => HPOEntrezFactory.informationAO.annotatingConcepts.map{c => c.value}.toList.sortWith(termSorter).toArray
      case SpecificHPOTerms(terms) => terms.toList.sortWith(termSorter).toArray
      case _ => throw new Exception("Unknown HPOTermSet")
    }
  }

  /**
   * computes the pairwise similarity for all terms in terms.
   * @return ArrayBuffer[(Double,Double)] first element is sim1, second element is sim2
   */
  def computePairWiseSimilarties(termSet: HPOTermSet) : ArrayBuffer[(Double, Double)] = {
    val terms = sortedTerms(termSet)
    val l = (terms.length * terms.length).toDouble
    val tic = Platform.currentTime
    var i = 1
    val N = ao.countAll
    (new scala.collection.mutable.ArrayBuffer[(Double,Double)]() /: terms){(b1, t1) =>
      val c1 = HPO_Term.getConceptById(t1.id).get
      val nt1 = ao.countDescendantFor(HPO_Term.getConceptById(t1.id).get).getOrElse(0)
      (b1 /: terms){(b2, t2) =>
        val c2 = HPO_Term.getConceptById(t2.id).get
        val nt2 = ao.countDescendantFor(HPO_Term.getConceptById(t2.id).get).getOrElse(0)
        val (c,nmica) = ao.minAnnotatedCommonAncestor(c1,c2)
        if((i % 1000000 == 0 && i<1e6) || (i % 1e6 ==0)) {
          val toc = Platform.currentTime
          val r = i/l
          val totalTimeEst = (toc-tic)/(1000.0 * 60.0)/r
          val eta = (1.0 - r)*totalTimeEst
          log.info(s"Terms: $l, Computed: $i, Percent: ${i/l*100}, Time: ${(toc-tic)/(1000.0 * 60.0)} min, TTE: $totalTimeEst min, ETA:$eta min")
        }
        i = i + 1
        val sim1 = Math.log(N/nmica.toDouble)
        val sim2 = nt1 * nt2 /(nmica*nmica).toDouble
        b2.+=((sim1, sim2))
      }
    }
  }

}

class Worker extends Actor{
  val log: LoggingAdapter = Logging(context.system, this)
  private val phenotypeIndicies = scala.collection.mutable.Map[Int,Set[Set[Int]]]()
  val rootStoragePath = ConfigUtil.applicationConfig("filestorage").getString("root")

  private def scoreAllModels(s1:Set[Int], s2:Set[Int], termSet: HPOTermSet) : (Int, Int, Int, Int) = {
    val (pairWiseSims, l) = termSet match{
      case AllHPOTerms => (Worker.hpoAllPairWiseSims, Worker.hpoAll.length)
      case AnnotatingHPOTerms => (Worker.hpoAnnotatingPairWiseSims, Worker.hpoAnnotating.length)
      case SpecificHPOTerms(terms) => (Worker.computePairWiseSimilarties(termSet), terms.length)
      case _ => throw new Exception("Unkwnon HPOTermsSet")
    }

    val aSymLeft = {
      val sums = ((0.0,0.0) /: s1){(partialSums, i1) =>
        val idx = l*i1
        //find max sim between term i1 and terms in s2
        val sims = ((0.0, 0.0) /: s2){(m, i2) =>
          val curSim1 = m._1
          val curSim2 = m._2
          val (sim1, sim2) = pairWiseSims(idx + i2)
          (Math.max(curSim1,sim1), Math.max(curSim2, sim2))
        }
        (partialSums._1 + sims._1, partialSums._2 + sims._2)
      }
      val S1N = s1.size.toDouble
      (sums._1/S1N, sums._2/S1N)
    }

    val aSymRight = {
      val sums = ((0.0,0.0) /: s2){(partialSums, i2) =>
        val idx = l*i2
        //find max sim between term i1 and terms in s2
        val sims = ((0.0, 0.0) /: s1){(m, i1) =>
          val curSim1 = m._1
          val curSim2 = m._2
          val (sim1, sim2) = pairWiseSims(idx + i1)
          (Math.max(curSim1,sim1), Math.max(curSim2, sim2))
        }
        (partialSums._1 + sims._1, partialSums._2 + sims._2)
      }
      val S2N = s2.size.toDouble
      (sums._1/S2N, sums._2/S2N)
    }

    val asym_sim1 = aSymLeft._1
    val asym_sim2 = aSymLeft._2
    val sym_sim1 = (asym_sim1 + aSymRight._1) * 0.5
    val sym_sim2 = (asym_sim2 + aSymRight._2) * 0.5
    (scale(asym_sim1), scale(sym_sim1), scale(asym_sim2), scale(sym_sim2))
  }

  def conceptsFromKeys(keys: Set[String]) = keys map {k => Worker.ao.hpoConceptById(k)} flatten

  private def scale(x:Double, sf:Int = 1000000) : Int = (x*sf).toInt

  private def updateScoreMap(m: Map[Int,Int], score: Int) = {
    val count = m.get(score) match{
      case Some(c) => c + 1
      case _ => 1
    }
    m.+((score->count))
  }

  def receive = {
    case op : Op => op match{

      case AddPhenotypeIndicesForJob(indicies, jobID) =>{
         phenotypeIndicies.get(jobID) match{
           case Some(ci) => phenotypeIndicies.+=(jobID->ci.++(indicies))
           case _ => phenotypeIndicies(jobID)=indicies
         }
      }

      case DropPhenotypeIndiciesForJob(jobID) => phenotypeIndicies -= jobID

      case ComputeAndStoreSimScoresAllModels(gene, termSet, jobID, k) =>{
        val termGeneMap = termSet match{
          case AllHPOTerms => Worker.hpoAllGeneIndices
          case AnnotatingHPOTerms => Worker.hpoAnnotatedGeneIndices
          case SpecificHPOTerms(terms) => Worker.mapGenesToHpoIndices(Set(gene),terms)
          case _ => throw new Exception(s"Unknown Term Set: $termSet")
        }

        phenotypeIndicies.get(jobID) match{
          case Some(samples) => {
            termGeneMap.get(gene) match{
              case Some(geneTermIds) => {
                val scores = ((Map[Int,Int](),Map[Int,Int](),Map[Int,Int](),Map[Int,Int]()) /: samples){(tpl,s) =>
                  val (as1, s1, as2, s2) = scoreAllModels(s, geneTermIds, termSet)
                  (updateScoreMap(tpl._1, as1),
                   updateScoreMap(tpl._2, s1),
                   updateScoreMap(tpl._3, as2),
                   updateScoreMap(tpl._4, s2))
                }
                val fs = FileStorage(rootStoragePath, log)
                fs.store(scores, gene, k)
                sender ! ComputeAndStoreSimScoresComplete(gene, jobID)
              }
              case _ => {
                log.error(s"Gene term Ids not found for gene: ${gene.id}")
                sender ! ComputeAndStoreSimScoresComplete(gene, jobID)
              }
            }
          }
          case _ => {
            log.error(s"No Phenotype Indices Received for jobID: $jobID, cannot compute similarity scores for gene: ${gene.id}")
            //TODO maybe upgrade this with a boolean to gene can be resent?
            sender ! ComputeAndStoreSimScoresComplete(gene, jobID)
          }
        }
      }

      case _ => sender ! UnhandledOp(op)
    }//end case Op

    case RemoteApplicationShutdown(force) => {
      log.info("Received Application Shutdown")
      if(force)context.system.shutdown()
    }

    case msg:Any => sender ! UnhandledMessageType(s"Unknown msg type ${msg}")
  }
}

class WorkerApplication extends Bootable{
  val config = ConfigUtil.applicationConfig("worker")
  val system = ActorSystem("WorkerApplication", config)
  val numWorkers = config.getInt("numWorkersPerNode")
  val router = system.actorOf(Props[Worker].withRouter(RoundRobinRouter(numWorkers)), "router")

  def startup(){}

  def shutdown(){
    //TODO add logging
    println("Shutting down Worker Application")

    system.shutdown()
  }

}

object WorkerApp{
  def run(){
    new WorkerApplication
    new StorageApplication
    println("Started Worker Application and Storage Application")
  }
}




