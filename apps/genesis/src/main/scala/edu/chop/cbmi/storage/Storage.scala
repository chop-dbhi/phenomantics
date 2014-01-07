package edu.chop.cbmi.storage

import edu.chop.cbmi._
import java.io._
import akka.event.{Logging, LoggingAdapter}
import edu.chop.cbmi.util.{ConfigUtil, Compression, S3}
import edu.chop.cbmi.phenomantics.api.gene.EntrezGene
import akka.kernel.Bootable
import akka.actor.{Props, Actor, ActorSystem}

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 4/12/13
 * Time: 2:31 PM
 */


case class FileStorage(rootPath: String, log: LoggingAdapter){

  def store(scores : (Map[Int,Int], Map[Int,Int], Map[Int,Int], Map[Int,Int]), gene: EntrezGene, k: Int) = {
    storeScoresToFile(k, AsymSim1, scores._1, gene)
    storeScoresToFile(k, SymSim1, scores._2, gene)
    storeScoresToFile(k, AsymSim2, scores._3, gene)
    storeScoresToFile(k, SymSim2, scores._4, gene)
  }

 def tarAndZip(source : String, destination:String) = {
   try{
     Compression.compressFile(new File(source), new File(destination))
   }catch{
     case e:Exception => log.error(e,s"Unable to create tar file for $source")
   }
 }

 def saveToS3(f:File, k:String)= {
   try{
     S3.putFileInBucket(f, k)
     log.info(s"Successfully moved ${f.getName} to S3")
   }catch{
     case e:Exception => log.error(e, s"Unable to store file ${f.getName} to S3 bucket ${S3.defaultBucketName} with credentials ${S3.awsCredentials.getAWSAccessKeyId()}")
   }
 }

  def storeScoresToFile(k: Int, simID: SimFuncId, scores: Map[Int, Int], gene: EntrezGene) = {
    val simS = simID.stringName
    val ks = if (k < 10) s"0${k}" else k.toString
    val geneId = gene.id
    val rp = if(rootPath.endsWith("/"))rootPath.dropRight(1) else rootPath
    val fn = s"${rp}/k_${ks}/${simS}/simscore_data_${simS}_k_${ks}_gene_${geneId}.dat"
    log.info(s"Storing File: $fn")
    printToFile(fn) {
      p =>
        scores foreach {
          score => p.println(s"${score._1}:${score._2}")
        }
    }
  }

 def printToFile(path: String)(op: PrintWriter => Unit) {
    val f = new File(path)
    if (f.exists) f.delete
    if (!f.getParentFile.exists())f.getParentFile.mkdirs()
    val pw = new PrintWriter(new File(path))
    try {
      op(pw)
    } finally {
      pw.close()
    }
  }

  def appendToFile(path: String)(op: PrintWriter => Unit) {
    val f = new File(path)
    if (!f.exists()){
      if(!f.getParentFile.exists())f.getParentFile.mkdirs()
      f.createNewFile()
    }
    val pw = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))
    try { {
      op(pw)
    }
    } finally {
      pw.close()
    }
  }
}

class StorageActor extends Actor{
  val log = Logging(context.system, this)

  def receive = {
    case(op:Op) => op match{
      case TarAndS3SimScores(rootpath, k, storageID, jobID) => {
        val ks = if (k<10) s"0${k}" else k.toString
        val fs = FileStorage(rootpath, log)
        fs.tarAndZip(s"${rootpath}/k_${ks}", s"./AllSimFunctions_k_${ks}_sid_${storageID}.tar.gz")
        if (S3.use_S3_?) fs.saveToS3(new File(s"./AllSimFunctions_k_${ks}_sid_${storageID}.tar.gz"), s"AllSimFunctions_k_${ks}_sid_${storageID}.tar.gz")
        sender ! TarAndS3SimScoresComplete(storageID, jobID)
      }

      case _ => sender ! UnhandledOp(op)
    }

    case RemoteApplicationShutdown(force) => {
      log.info("Received Application Shutdown")
      if(force)context.system.shutdown()
    }

    case msg: Any => sender ! UnhandledMessageType(s"Unknown msg type: ${msg}")
  }
}

class StorageApplication extends Bootable{
  val config = ConfigUtil.applicationConfig("storage")
  val system = ActorSystem("StorageApplication", config)
  val storageActor = system.actorOf(Props[StorageActor], "storageActor")
  def startup(){}

  def shutdown(){

  }
}

object StorageApp{
  def run(){
    new StorageApplication
    println("Started Storage Applications")
  }
}
