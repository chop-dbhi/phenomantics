package edu.chop.cbmi

import akka.kernel.Bootable
import akka.actor.ActorRef
import edu.chop.cbmi.phenomantics.api.ontologies.HPO_Term
import edu.chop.cbmi.phenomantics.api.gene.EntrezGene

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 4/5/13
 * Time: 9:40 AM
 */


trait GeneList
case object AllAnnotatedEntrezGenes extends GeneList
case class SpecificGenes(ids: Seq[Int]) extends GeneList

trait HPOTermSet
case object AllHPOTerms extends HPOTermSet
case object AnnotatingHPOTerms extends HPOTermSet
case class SpecificHPOTerms(terms: Array[HPO_Term]) extends HPOTermSet

trait SimFuncId {
  val stringName : String
}
case object AsymSim1 extends SimFuncId {
  val stringName = "ASymSim1"
}
case object SymSim1 extends SimFuncId {
  val stringName= "SymSim1"
}
case object SymSim2 extends SimFuncId {
  val stringName = "SymSim2"
}
case object AsymSim2 extends SimFuncId{
  val stringName = "ASymSim2"
}
case object AllSimilarityFunctions extends SimFuncId {
  val stringName = "AllSimilarityFunctions"
}

trait Job
case class KJob(k:Int, N: Int, genes: GeneList, simFunc: SimFuncId, jobMonitor: ActorRef, hpoTerms: HPOTermSet) extends Job
case class ApplicationShutdown(application : Bootable, force: Boolean = false) extends Job
case class RemoteApplicationShutdown(force:Boolean) extends Job

trait JobStatus
case class JobFailed(job: Job) extends JobStatus
case class JobComplete(job: Job) extends JobStatus

trait Op {
  val jobID : Int
}
case class AddPhenotypeIndicesForJob(indices: Set[Set[Int]], jobID: Int) extends Op
case class DropPhenotypeIndiciesForJob(jobID:Int) extends Op
case class ComputeAndStoreSimScoresAllModels(gene: EntrezGene, hpoTerms: HPOTermSet, jobID : Int, k: Int) extends Op
case class TarAndS3SimScores(rootPath: String, k: Int, storageID: Int, jobID:Int) extends Op

trait Result {
  val jobID : Int
}
case class ComputeAndStoreSimScoresComplete(gene: EntrezGene, jobID : Int) extends Result
case class JobManagerJobComplete(jobID: Int) extends Result
case class TarAndS3SimScoresComplete(storageID :Int, jobID: Int) extends Result

trait Error
case class UnhandledMessageType(msg : Any) extends Error
case class UnhandledJobType(job:Job) extends Error
case class UnhandledOp(op: Op) extends Error
case class UnhandledResultType(r: Result) extends Error
case class UnknownJobNumber(jobID: Int, msg : Option[String] = None) extends Error

