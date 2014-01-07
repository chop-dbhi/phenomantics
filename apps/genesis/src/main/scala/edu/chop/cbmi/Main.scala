package edu.chop.cbmi

import edu.chop.cbmi.phenomantics.api.gene.EntrezGene
import scala.compat.Platform


/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 6/7/13
 * Time: 4:25 PM
 */
object Main {
  def main(args: Array[String]){
    val appNum = if(args.length<1){
      readApplicationNumberFromUser()
    }else{
      processSelection(args(0)) match{
        case Right(i) => i
        case Left(s) => {
          println(s)
          readApplicationNumberFromUser()
        }
      }
    }

    if(appNum==1){
      val kvalsOpt = if(args.length<2)readKVals()
      else processKVals(args(1))

      kvalsOpt match{
        case Some(kvals) =>{
          val NOpt = if(args.length<3)readN()
          else processN(args(2))

          NOpt match{
            case Some(n) => {
               val sfvalsOpt = if(args.length<4)readSimFunc
                 else processSimFunc(args(3))

              sfvalsOpt match{
                case Some(sfvals)=> Genesis.run(n,kvals,sfvals)
                case _ => println("SimFunction value must be supplied. Application shutting down.")
              }
            }
            case _ => println("A value for N must be supplied. Application shutting down.")
          }
        }
        case _ => println("KVals must be supplied. Application shutting down.")
      }

    }
    else if(appNum==2)JobManagerApp.run()
    else if(appNum==3)WorkerApp.run()
    else if(appNum==4)PreComputeApplication.serializeHPOTermPairwiseSims()
    else println("Application Stopping.")
  }

  def readSimFunc = {
    println("Please select a similarity function choice, separate multiple choices with a , :")
    println("[1] ASymSim1")
    println("[2] SymSim1")
    println("[3] SymSim2")
    println("[4] ASymSim2")
    println("[5] All")
    processSimFunc(s"sf=${readLine()}")
  }

  def processSimFunc(input: String) : Option[Set[SimFuncId]] = {
    val trimmed = input.trim.toLowerCase
    val reg = "^sf=(.+)".r
    if(reg.findFirstMatchIn(trimmed).isEmpty)None
    else{
      val reg(sfvals) = trimmed
      Some(sfvals.split(",").map{is =>
        if(is=="1")Some(AsymSim1)
        else if(is=="2")Some(SymSim1)
        else if(is=="3")Some(SymSim2)
        else if(is=="4")Some(AsymSim2)
        else if(is=="5")Some(AllSimilarityFunctions)
        else None
      }.flatten.toSet)
    }
  }

  def readN() = {
      println("Please enter the number, N, of phenotype query samples:")
      processN(s"N=${readLine()}")
  }

  def processN(input: String) : Option[Int] = {
    val trimmed = input.trim.toLowerCase()
    val reg = "^n=(\\d+)$".r
    if(reg.findFirstMatchIn(trimmed).isEmpty)None
    else{
      val reg(n) = trimmed
      Some(n.toInt)
    }
  }

  def readKVals() = {
    println("The Genesis Application requires k val inputs.")
    println("Please enter a comma separated list of k values, e.g. 1,2,3 and then press enter")
    processKVals(s"k=${readLine()}")
  }

  def processKVals(input: String) : Option[Set[Int]] = {
    val trimmed = input.trim.toLowerCase
    val reg = "^k=(.+)".r
    if(reg.findFirstMatchIn(trimmed).isEmpty)None
    else{
      val reg(kvals) = trimmed
      Some(kvals.split(",").map(_.toInt).toSet)
    }

  }

  def readApplicationNumberFromUser() : Int = {

    println("Please select the application you wish to run:")
    println("[1] Genesis")
    println("[2] JobManager")
    println("[3] Worker")
    println("[4] HPO Term Pairwise Similarity Score Serialization")
    println("[5] Quit Application")
    processSelection(readLine()) match{
      case Right(i) => i
      case Left(s) => {
        println(s)
        readApplicationNumberFromUser()
      }
    }
  }

  def processSelection(selection: String) : Either[String, Int] = {
    try{
      val i = selection.trim.toInt
      if(i>0 && i<6)Right(i)
      else Left("Selection must be an integer in [1,5]")
    }catch{
      case nfe: NumberFormatException => Left("Selection must be an integer")
      case e : Exception => Left("Please enter a valid selection")
    }
  }
}
