package edu.chop.cbmi

import java.io.{ObjectInputStream, FileInputStream, ObjectOutputStream, FileOutputStream}

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 6/24/13
 * Time: 11:46 AM
 */
object PreComputeApplication {

  val defaultHPOPairFilePath = "hpoPairSims.ser"
  val defaultGeneHPOIndexFilePath = "hpoGeneMapIndex.ser"

  def serialize(outputFilePath: String, o : Any) = {
    val fileOut = new FileOutputStream(outputFilePath)
    val out = new ObjectOutputStream(fileOut)
    out.writeObject(o)
    out.close
    fileOut.close()
  }

  def serializeHPOTermPairwiseSims(outputFilePath : String = defaultHPOPairFilePath, termSet : HPOTermSet = AnnotatingHPOTerms) = {
    val hpoPairSims = Worker.computePairWiseSimilarties(termSet)
    serialize(outputFilePath, hpoPairSims)
  }

  def deserialize[T](inputFilePath:String)(implicit  m : Manifest[T]) = {
    val fileIn = new FileInputStream(inputFilePath)
    val in = new ObjectInputStream(fileIn)
    val hpoPairSims = in.readObject().asInstanceOf[T]
    in.close()
    fileIn.close()
    hpoPairSims
  }
}
