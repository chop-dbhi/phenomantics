/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 8/27/13
 * Time: 11:06 AM
 */

import edu.chop.cbmi.phenomantics.api.gene.{EntrezGene, EntrezGeneSet}
import edu.chop.cbmi.phenomantics.api.ontologies._
import edu.chop.cbmi.phenomantics.rest.calculator.{AnnotatedHPO, GeneDistance}
import java.io.{FileWriter, BufferedWriter, File, PrintWriter}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform
import scala.io.Source
import scala.language.postfixOps
import scala.util.parsing.json.JSON
import scala.util.Random


//OPTIONS -----------------------------------------------------------
val runStaging = false
val runSingleDisease = false
val runSingleDiseaseAllCases = false
val runSingleDiseasePvals = false
val runSingleDiseasePostProcessing = false
val runSingleDiseaseKBoundeddPostProcessing = true
val runPatientStats = false
val runPolyDisease = false
val numPatients = 100
//END OPTIONS -------------------------------------------------------

trait Gender
object Male extends Gender{
  override def toString = "M"
}
object Female extends Gender{
  override def toString = "F"
}
object GenderNA extends Gender{
  override def toString = "NA"
}

case class HPOFreq(id:String, freq: Double, gender:Gender) {

  val term = HPO_Term.getTermById(id).get

  def toJson = s"""{"term":"$id","freq":$freq,"gender":"$gender"}"""

}

case class OMIMEntry(id:String, name:String, geneIds:List[String]){

  val genes = geneIds.map{id=> EntrezGeneSet.hpoAnnotatedGeneFor(id).get}

  def toJson = s""""OMIM":{"id":"$id", "name":"$name", "genes":[${(""/:geneIds){(s,n)=>s + "\"" + n +"\"" + ","}.dropRight(1)}]}"""

}

case class Disease(omim : OMIMEntry, hpoFreqs : List[HPOFreq])

case class Patient(disease : Disease, phenotypes: List[HPO_Term], gender: Gender)

case class ScoredGene(gene: EntrezGene, simscores : (Double, Double, Double, Double), pvalues : (Double, Double, Double, Double)){
  /**
   * use [0,3] for sim score [4,7] for pval
   * @param scoreIndex
   */
  def scoreFor(scoreIndex:Int): Double = {
    val idx = if(scoreIndex<4)scoreIndex else scoreIndex-4
    val usePval = scoreIndex>3
    (if(usePval)pvalues.productElement(idx) else simscores.productElement(idx)).asInstanceOf[Double]
  }
}

case class SimScoreFileReader(file: File){
  val lines = Source.fromFile(file).getLines()

  val (gender, numPhenotypes, phenotypes, optimalToActualSims) = {
    /*
              pw.println(s"${optToActSim._1},${optToActSim._2},${optToActSim._3},${optToActSim._4}")
     */
    //skip header 1: ##Gender, Num Phenotypes, Phenotypes
    lines.next()
    //get phenotype meta
    val pm = lines.next.split(",")
    val gen = if(pm(0).trim == "M")Male else if(pm(0).trim == "F")Female else GenderNA
    val nps = pm(1).trim.toInt
    val pts = (List[String]() /: Range(2, pm.size))((l,i)=> pm(i) :: l)

    // skip headers 2 ##Semantic Similarities Patient Actual to Patient Optimal
    // skip header 3 SS_AS1, SS_S1, SS_AS2, SS_S2
    lines.next()
    lines.next()
    //get optimal to actual
    val opt2act = lines.next.split(",")

    //skip header 4 ##GeneSym, SS_AS1, SS_S1, SS_AS2, SS_S2
    lines.next()   //sets reader to correct position

    (gen, nps, pts, opt2act)
  }
}

/*STAGING WORK
convert Kohler diseases to gene disease, i.e. if disease gene does not have the disease annotation OR annotation term no longer
in HPO remove it from disease list as this will be noise for the gene
record the removed hpo terms
separate multi-gene diseases
*/
lazy val ontology = {
  log("Loading ontology ...")
  val o = HPOEntrezFactory.informationAO
  log("Done loading ontology ...")
  o
}

if(runStaging){
  log("starting staging code ...")
  val kholerFile = Source.fromFile("./conf/KholerDiseases.json")
  kholerFile.getLines().foreach{line =>
    JSON.parseFull(line) match{
       case Some(m:Map[String,Any]) => {

         val omim : OMIMEntry = extractOmim(m) match{
           case Some(oe) => oe
           case _ => throw new Exception("invalid line: %s".format(line))
         }

         val annotations: Set[Concept[String, HPO_Term]] = (Set[Concept[String,HPO_Term]]() /: omim.genes){(s, g) =>
           ontology.annotationsFor(g) match{
             case Some(a) => s.++(a)
             case _ => s
           }
         }

         val hpoFreqs: List[HPOFreq] = m.get("HPO_FREQ") match {
           case Some(l: List[Map[String, Any]]) => {
             l.map { entry =>
               extractHpoFreq(entry, omim, annotations) match{
                 case Right(hf) => Some(hf)
                 case Left(id) => {
                   recordDroppedTerm(omim, id)
                   None
                 }
               }
             }.flatten  //end l.map
           }//end case Some(l)
           case _ => throw new Exception("invalid line %s".format(line))
         }//end creation of hpo freqs variable

         if(hpoFreqs.length>0){
           val dir = if(omim.geneIds.length==1)"./data/diseases/final/single" else "./data/diseases/final/poly"
           val f = s"$dir/diseases.json"
           //println(toJson(omim, hpoFreqs))
           appendToFile(f){pw => pw.println(toJson(omim, hpoFreqs))}

         }else{
           //drop this disease
           val f = s"./data/diseases/droppedDiseases.txt"
           appendToFile(f){pw => println(omim.id)}
         }

       }
       case _ => log("ERROR: Cannot Parse Line\n%s".format(line))
     }
  }
} //end runStaging

/*SINGLE CAUSATIVE GENE SIMULATIONS
creates artificial patients for diseases with only a single known causative gene
uses noise and/or imprecision options
for each disease stores a file where rows are:
causative gene simscore, causative gene p-value, causative gene rank by simscore, causative gene rank by p-value
*/
if(runSingleDisease){
  //################ OPTIONS ###############
  val applyNoise = false
  val applyImprecision = false
  //#######################################

  log("starting single disease simulation code ...")
  val diseaseFile = Source.fromFile("./data/diseases/final/single/diseases.json")
  val asymSim1 = ontology.AsymMaxSim1
  val asymSim2 = ontology.AsymMaxSim2
  val symSim1 = ontology.SymMaxSim1
  val symSim2 = ontology.SymMaxSim2
  diseaseFile.getLines.foreach{line =>
    JSON.parseFull(line) match{
      case Some(m:Map[String,Any]) =>{

        val omim : OMIMEntry = extractOmim(m) match{
          case Some(oe) => oe
          case _ => throw new Exception("invalid line: %s".format(line))
        }

        val hpoFreqs: List[HPOFreq] = m.get("HPO_FREQ") match {
          case Some(l: List[Map[String, Any]]) => {
            l.map { entry =>
              extractHpoFreqUnchecked(entry, omim) match{
                case Right(hf) => Some(hf)
                case Left(id) => {
                  log(s"WARNING: HPO term $id for disease ${omim.id} not in ontology")
                  None
                }
              }
            }.flatten  //end l.map
          }//end case Some(l)
          case _ => throw new Exception("invalid line %s".format(line))
        }//end creation of hpo freqs variable

        val disease = Disease(omim, hpoFreqs)
        log(s"Parsed disease ${disease.omim.id}")
        val patients = diseasePatients(numPatients, disease, applyNoise, applyImprecision)
        log(s"Created patients")
        val diseaseDir = diseaseDirFor(disease.omim.id, applyNoise, applyImprecision)

        val start = Platform.currentTime
        patients.zipWithIndex.foreach{tpl =>
          val patient = tpl._1
          val idx = tpl._2
          val file = s"$diseaseDir/patient_$idx.dat"
          appendToFile(file){pw=>
            pw.println("Gender,Phenotypes")
            pw.println(s"${patient.gender},${patient.phenotypes.length}, ${(""/:patient.phenotypes){(s,p)=>s+p.id+","}.dropRight(1)}")
            pw.println("GeneSym, SS_AS1, SS_S1, SS_AS2, SS_S2")
          }

          val tic = Platform.currentTime
          EntrezGeneSet.hpo_annotated_genes.foreach{gene =>
            val ss = GeneDistance.similarityAllModels(patient.phenotypes.toSet, gene)
            appendToFile(file){pw =>
              pw.println(s"${gene.symbol},${ss._1},${ss._2},${ss._3},${ss._4}")
            }
          }
          val toc = Platform.currentTime
          val total = (toc-start)/1000.
          val thisPatient = (toc-tic)/1000.
          val eta = total * (numPatients/(idx+1).toDouble - 1)
          log(s"Simulated patient ${idx+1} of $numPatients took $thisPatient sec, eta = $eta sec")
        }
      }
      case _ => log("ERROR: Cannot Parse Line\n%s".format(line))
    }
  }
}

//-------------------------UPDATED VERSION ------------------------
if(runSingleDiseaseAllCases){
  log("starting single disease simulation code ...")
  val diseaseFile = Source.fromFile("./data/diseases/final/single/diseases.json")
  val asymSim1 = ontology.AsymMaxSim1
  val asymSim2 = ontology.AsymMaxSim2
  val symSim1 = ontology.SymMaxSim1
  val symSim2 = ontology.SymMaxSim2
  val NOISE = true
  val IMPRECISION = true
  val noiseImprecisionPermutations = List((!NOISE, !IMPRECISION), (NOISE, !IMPRECISION), (!NOISE, IMPRECISION), (NOISE, IMPRECISION) )

  diseaseFile.getLines.foreach{line =>
    JSON.parseFull(line) match{
      case Some(m:Map[String,Any]) =>{

        val omim : OMIMEntry = extractOmim(m) match{
          case Some(oe) => oe
          case _ => throw new Exception("invalid line: %s".format(line))
        }

        val hpoFreqs: List[HPOFreq] = m.get("HPO_FREQ") match {
          case Some(l: List[Map[String, Any]]) => {
            l.map { entry =>
              extractHpoFreqUnchecked(entry, omim) match{
                case Right(hf) => Some(hf)
                case Left(id) => {
                  log(s"WARNING: HPO term $id for disease ${omim.id} not in ontology")
                  None
                }
              }
            }.flatten  //end l.map
          }//end case Some(l)
          case _ => throw new Exception("invalid line %s".format(line))
        }//end creation of hpo freqs variable

        val disease = Disease(omim, hpoFreqs)
        val eligibleNoiseTerms = diseaseEligibleNoiseTerms(disease)
        log(s"** Parsed disease ${disease.omim.id}")
        val patients = createOptimalPatients(numPatients, disease)
        log(s"Created patients")

        val start = Platform.currentTime
        patients.zipWithIndex.foreach{tpl =>
          val patient = tpl._1
          val idx = tpl._2
          val tic = Platform.currentTime

          val optimalTerms = patient.phenotypes
          val impreciseTerms = addImprecision(optimalTerms)
          val noiseTerms = addNoise(optimalTerms, eligibleNoiseTerms)
          val noiseImpreciseTerms = (noiseTerms.filterNot{t=>optimalTerms.contains(t)} ::: impreciseTerms).toSet.toList

          noiseImprecisionPermutations.foreach{nitpl =>
            val noise = nitpl._1
            val imprecision = nitpl._2
            val diseaseDir = diseaseDirFor(disease.omim.id, noise, imprecision)

            val file = s"$diseaseDir/patient_$idx.dat"

            val patientPhenotypes = if(noise && imprecision)noiseImpreciseTerms
              else if(noise) noiseTerms
              else if(imprecision) impreciseTerms
              else optimalTerms

            val optimalInts = patient.phenotypes.map{hpoTerm => AnnotatedHPO.hpoAnnotating.indexOf(hpoTerm)}.filterNot{_==(-1)}.toSet
            val actualInts = patientPhenotypes.map{hpoTerm => AnnotatedHPO.hpoAnnotating.indexOf(hpoTerm)}.filterNot{_==(-1)}.toSet
            val optToActSim: (Double, Double, Double, Double) = GeneDistance.similarityAllModels(actualInts, optimalInts)

            appendToFile(file){pw=>
              pw.println("##Gender, Num Phenotypes, Phenotypes")
              pw.println(s"${patient.gender},${patientPhenotypes.length}, ${(""/:patientPhenotypes){(s,p)=>s+p.id+","}.dropRight(1)}")
              pw.println("##Semantic Similarities Patient Actual to Patient Optimal")
              pw.println("SS_AS1, SS_S1, SS_AS2, SS_S2")
              pw.println(s"${optToActSim._1},${optToActSim._2},${optToActSim._3},${optToActSim._4}")
              pw.println("##GeneSym, SS_AS1, SS_S1, SS_AS2, SS_S2")
            }

            EntrezGeneSet.hpo_annotated_genes.foreach{gene =>
              val ss = GeneDistance.similarityAllModels(patientPhenotypes.toSet, gene)
              appendToFile(file){pw =>
                pw.println(s"${gene.symbol},${ss._1},${ss._2},${ss._3},${ss._4}")
              }
            }
          }//end loop over noiseImprecisionPermutations
          val toc = Platform.currentTime
          val total = (toc-start)/1000.
          val thisPatient = (toc-tic)/1000.
          val eta = total * (numPatients/(idx+1).toDouble - 1)

          log(s"Simulated patient ${idx+1} of $numPatients took $thisPatient sec, eta = $eta sec")
        } // end patients zip with index

        log(s"*** Completed Disease ${disease.omim.id}")
      }
      case _ => log("ERROR: Cannot Parse Line\n%s".format(line))
    }
  }
}

if (runSingleDiseasePvals) {
  //################ OPTIONS ###############
  val applyNoise = false
  val applyImprecision = false
  //#######################################
  log("Starting single disease pval calculations ...")
  //create a list of file reader objects -> combines file name and getLines
  val dirFile = new File(diseaseDirFor("", applyNoise, applyImprecision))
  val diseaseDirs = dirFile.listFiles().toList

  val patientFiles = diseaseDirs.map{_.listFiles.toList}.flatten.map{file => SimScoreFileReader(file)}

  val start = Platform.currentTime
  val numGenes = EntrezGeneSet.hpo_annotated_genes.size
  var counter = 0.0

  EntrezGeneSet.hpo_annotated_genes.foreach{gene =>

    val tic = Platform.currentTime

    //hold 4 maps of int( phenotype qry size) -> CDF table
    val asym1CDF = scala.collection.mutable.Map[Int, Array[(Double, Double)]]()
    val sym1CDF = scala.collection.mutable.Map[Int, Array[(Double, Double)]]()
    val asym2CDF = scala.collection.mutable.Map[Int, Array[(Double, Double)]]()
    val sym2CDF = scala.collection.mutable.Map[Int, Array[(Double, Double)]]()

    patientFiles.foreach{fr =>
      //read next line in file -> compute CDF, get new table if needed
      val line = fr.lines.next().split(",")
      val g = line(0).trim
      if(g != gene.symbol)log(s"ERROR: mismatched gene for file ${fr.file.getName}")

      //get the similarity scores
      val as1 = line(1).trim.toDouble
      val ss1 = line(2).trim.toDouble
      val as2 = line(3).trim.toDouble
      val ss2 = line(4).trim.toDouble

      val as1pval = pvalFor(asym1CDF, as1, g, fr.numPhenotypes, ontology.AsymMaxSim1)
      val s1pval = pvalFor(sym1CDF, ss1, g, fr.numPhenotypes, ontology.SymMaxSim1)
      val as2pval = pvalFor(asym2CDF, as2, g, fr.numPhenotypes, ontology.AsymMaxSim2)
      val s2pval = pvalFor(sym2CDF, ss2, g, fr.numPhenotypes, ontology.SymMaxSim2)

      //write pvals to file
      appendToFile(s"${fr.file.getCanonicalPath.dropRight(4)}_pvals.dat"){ pw =>
        pw.println(s"${gene.symbol},$as1pval, $s1pval, $as2pval, $s2pval")
      }

    }//end patientFiles loop

    counter = counter + 1.0
    val toc = Platform.currentTime
    val total = (toc-start)/1000.
    val thisGene = (toc-tic)/1000.
    val eta = total * (numGenes/counter - 1)
    log(s"Computed pvals for $counter of $numGenes took $thisGene sec, eta = $eta sec")

  }//end Entrez Gene loop
}//end if runSingleDiseasePvals

if(runSingleDiseasePostProcessing){
  //################ OPTIONS ###############
  val applyNoise = true
  val applyImprecision = true
  val recordRanks = false //record the score and pval rank of the causative gene to a file
  val recordTopRankPvalScore = true //record the score and pval of the top ranked
  //#######################################
  println("starting single disease post processing ...")
  val diseaseFile = Source.fromFile("./data/diseases/diseaseInfo/final/single/diseases.json")
  val sfile = "patient_%d.dat"
  val pfile = "patient_%d_pvals.dat"
  diseaseFile.getLines.foreach{line =>
    JSON.parseFull(line) match{
      case Some(m:Map[String,Any]) =>{

        val omim : OMIMEntry = extractOmim(m) match{
          case Some(oe) => oe
          case _ => throw new Exception("invalid line: %s".format(line))
        }
        //loop over patients
        val diseaseDir = diseaseDirFor(omim.id, applyNoise, applyImprecision)
        println(s"Starting disease $diseaseDir")
        for(i <- 0 until numPatients){
          //get sim scores
          val slines = Source.fromFile(s"$diseaseDir/${sfile.format(i)}").getLines.drop(6)
          val plines = Source.fromFile(s"$diseaseDir/${pfile.format(i)}").getLines
          val scoredGenes = (List[ScoredGene]() /: slines){(l,sline) =>
            val (g1, simScores) = splitScoringLine(sline)
            val (g2, pVals) = splitScoringLine(plines.next())
            if(g1.id==g2.id)ScoredGene(g1, simScores, pVals) :: l
            else{
              println(s"ERROR: Gene Symbol Mismatch for patient $i on disease ${omim.id}")
              l
            }
          }

          //val offset = scoredGenes.length + 1 //use for simscores ranks
          val gene = omim.genes.head

          if(recordRanks){
            val ssRanks = Range(0,4).map{i=>simScoreRankGene(i, scoredGenes.sortBy(sg=>sg.scoreFor(i)).reverse.iterator, gene)}
            val pRanks = Range(4,8).map{i=>pValRankGene(i, scoredGenes.sortBy(sg=>sg.scoreFor(i)).iterator, gene)}

            appendToFile(s"$diseaseDir/rankings.dat"){pw =>
              pw.println(s"${(""/:ssRanks){(s,r)=>s+r+","}}${(""/:pRanks){(s,r)=>s+r+","}.dropRight(1)}")
            }
          }

          if(recordTopRankPvalScore){
            val topScores = Range(0,4).map{i=> scoredGenes.sortBy(sg=>sg.scoreFor(i)).reverse.head.scoreFor(i)}
            val topPvals = Range(4,8).map{i=> scoredGenes.sortBy(sg=>sg.scoreFor(i)).head.scoreFor(i)}
            appendToFile(s"$diseaseDir/topPvalsScores.dat"){pw =>
              pw.println(s"${(""/:topScores){(s,r)=>s+r+","}}${(""/:topPvals){(s,r)=>s+r+","}.dropRight(1)}")
            }
          }

        }//LOOP ON PATIENTS ***************
      } //end case
      case _ => println("ERROR: Cannot Parse Line\n%s".format(line))
    }
  }
  System.out.println("Done.")
}

/**
 * this does the same gene ranking analysis as the runSingleDiseasePostProcessing block except that
 * it limits analysis to patients with K or less annotations
 */
if(runSingleDiseaseKBoundeddPostProcessing){
  //################ OPTIONS ###############
  val applyNoise = false
  val applyImprecision = true
  val recordRanks = true //record the score and pval rank of the causative gene to a file
  val recordTopRankPvalScore = true //record the score and pval of the top ranked
  val recordDiseasePatientCounts = true
  val klim = 2 //only patients with k or fewer annotations will be considered
  //#######################################

  println("starting single disease post processing ...")
  val diseaseFile = Source.fromFile("./data/diseases/diseaseInfo/final/single/diseases.json")
  val sfile = "patient_%d.dat"
  val pfile = "patient_%d_pvals.dat"
  val diseasePatientCounts = scala.collection.mutable.Map[String, Int]()
  diseaseFile.getLines.foreach{line =>
    JSON.parseFull(line) match{
      case Some(m:Map[String,Any]) =>{

        val omim : OMIMEntry = extractOmim(m) match{
          case Some(oe) => oe
          case _ => throw new Exception("invalid line: %s".format(line))
        }
        //loop over patients
        val diseaseDir = diseaseDirFor(omim.id, applyNoise, applyImprecision)
        println(s"Starting disease $diseaseDir")
        for(i <- 0 until numPatients){
          //get sim scores
          val slines = Source.fromFile(s"$diseaseDir/${sfile.format(i)}").getLines.drop(1)
          val phenoLineSplit = slines.next().split(",")
          val numPheno = phenoLineSplit(1).toInt

          if(numPheno<=klim){

            diseasePatientCounts.get(omim.id) match{
              case Some(count) => diseasePatientCounts.+=((omim.id, count+1))
              case _ => diseasePatientCounts.+=((omim.id, 1))
            }
            slines.drop(4) //skip remaining header info

            val plines = Source.fromFile(s"$diseaseDir/${pfile.format(i)}").getLines

            val scoredGenes = (List[ScoredGene]() /: slines){(l,sline) =>
              val (g1, simScores) = splitScoringLine(sline)
              val (g2, pVals) = splitScoringLine(plines.next())
              if(g1.id==g2.id)ScoredGene(g1, simScores, pVals) :: l
              else{
                println(s"ERROR: Gene Symbol Mismatch for patient $i on disease ${omim.id}")
                l
              }
            }

            //val offset = scoredGenes.length + 1 //use for simscores ranks
            val gene = omim.genes.head

            if(recordRanks){
              val ssRanks = Range(0,4).map{i=>simScoreRankGene(i, scoredGenes.sortBy(sg=>sg.scoreFor(i)).reverse.iterator, gene)}
              val pRanks = Range(4,8).map{i=>pValRankGene(i, scoredGenes.sortBy(sg=>sg.scoreFor(i)).iterator, gene)}

              appendToFile(s"$diseaseDir/rankings_klim_${klim}.dat"){pw =>
                pw.println(s"${(""/:ssRanks){(s,r)=>s+r+","}}${(""/:pRanks){(s,r)=>s+r+","}.dropRight(1)}")
              }
            }

            if(recordTopRankPvalScore){
              val topScores = Range(0,4).map{i=> scoredGenes.sortBy(sg=>sg.scoreFor(i)).reverse.head.scoreFor(i)}
              val topPvals = Range(4,8).map{i=> scoredGenes.sortBy(sg=>sg.scoreFor(i)).head.scoreFor(i)}
              appendToFile(s"$diseaseDir/topPvalsScores_klim_${klim}.dat"){pw =>
                pw.println(s"${(""/:topScores){(s,r)=>s+r+","}}${(""/:topPvals){(s,r)=>s+r+","}.dropRight(1)}")
              }
            }

          }//end if numPheno<klim
        }//LOOP ON PATIENTS ***************
      } //end case
      case _ => println("ERROR: Cannot Parse Line\n%s".format(line))
    }//end JSON Parse match
  }//end diseaseFile.getLines
  val diseaesDir = diseaseDirFor("", applyNoise, applyImprecision)
  appendToFile(s"$diseaesDir/diseasePatientCount_klim_${klim}"){pw =>
    diseasePatientCounts.foreach{tpl => pw.println(tpl._1, tpl._2)}
  }
  System.out.println("Done.")
}

if(runPatientStats){
  //################ OPTIONS ###############
  val applyNoise = true
  val applyImprecision = true
  //#######################################
  println("starting single disease post processing ...")
  val diseaseFile = Source.fromFile("./data/diseases/diseaseInfo/final/single/diseases.json")
  val sfile = "patient_%d.dat"
  val phenoCountMap = scala.collection.mutable.Map[Int,Int]()
  diseaseFile.getLines.foreach{line =>
    JSON.parseFull(line) match{
      case Some(m:Map[String,Any]) =>{

        val omim : OMIMEntry = extractOmim(m) match{
          case Some(oe) => oe
          case _ => throw new Exception("invalid line: %s".format(line))
        }
        //loop over patients
        val diseaseDir = diseaseDirFor(omim.id, applyNoise, applyImprecision)
        println(s"Starting disease $diseaseDir")
        for(i <- 0 until numPatients){
          //get sim scores
          val patientInfo = Source.fromFile(s"$diseaseDir/${sfile.format(i)}").getLines.drop(1).next()
          val split = patientInfo.split(",")
          val numPhenotypes = split(1).toInt
          phenoCountMap.get(numPhenotypes) match{
            case Some(c) => phenoCountMap.+=((numPhenotypes, c+1))
            case _ => phenoCountMap.+=((numPhenotypes,1))
          }

        }//LOOP ON PATIENTS ***************
      } //end case
      case _ => println("ERROR: Cannot Parse Line\n%s".format(line))
    }
  }
  val f = s"./data/diseases/patients/stats/patientPhenotypeCounts_noise_${applyNoise}_imp_${applyImprecision}.dat"
  appendToFile(f){pw =>
    pw.println("K, count")
    phenoCountMap.toList.sortBy{tpl=>tpl._1}.foreach{tpl=> pw.println(s"${tpl._1},${tpl._2}")}
  }
  System.out.println("Done.")
}

def simScoreRankGene(scoreIndex:Int, sortedGenes:Iterator[ScoredGene], gene:EntrezGene,
                     currentGeneIndex: Int = 0,
                     currentRankSum:Int = 0,
                     currentTieCount:Int = 0,
                     geneScore:Double = Double.NaN, geneFoundPreviously : Boolean = false) : Int = {
  if(sortedGenes.hasNext){
    val nextGene = sortedGenes.next()
    val idx = currentGeneIndex + 1
    val nextScore = nextGene.scoreFor(scoreIndex)
    val geneFoundNow = (nextGene.gene.id == gene.id)
    val geneFound = geneFoundNow || geneFoundPreviously
    val scoresMatch = nextScore == geneScore
    if(scoresMatch)simScoreRankGene(scoreIndex, sortedGenes, gene, idx, currentRankSum + idx, currentTieCount+1, geneScore, geneFound)
    else if(geneFoundNow) simScoreRankGene(scoreIndex, sortedGenes, gene, idx, idx, 1, nextScore, true)
    else if(!geneFoundPreviously) simScoreRankGene(scoreIndex, sortedGenes, gene, idx, idx, 1, nextScore, false)
    else currentRankSum/currentTieCount


  }else currentRankSum/currentTieCount
}


def pValRankGene(scoreIndex:Int, sortedGenes:Iterator[ScoredGene], gene:EntrezGene,
                     currentRank: Int = 0,
                     geneScore:Double = Double.NaN,
                     geneFoundPreviously : Boolean = false,
                     tiedGenes:Set[ScoredGene]=Set[ScoredGene]().empty) : Int = {
  if(sortedGenes.hasNext){
    val nextGene = sortedGenes.next()
    val nextScore = nextGene.scoreFor(scoreIndex)
    val geneFoundNow = (nextGene.gene.id == gene.id)
    val geneFound = geneFoundNow || geneFoundPreviously
    val scoresMatch = nextScore == geneScore
    val nextTiedGenes = if(scoresMatch) tiedGenes.+(nextGene)
                        else if(geneFoundPreviously) tiedGenes
                        else Set(nextGene)
    if(scoresMatch)pValRankGene(scoreIndex, sortedGenes, gene, currentRank+1, geneScore, geneFound, nextTiedGenes)
    else if(geneFoundNow) pValRankGene(scoreIndex, sortedGenes, gene, currentRank+1, nextScore, true, nextTiedGenes)
    else if(!geneFoundPreviously) pValRankGene(scoreIndex, sortedGenes, gene, currentRank+1, nextScore, false, nextTiedGenes)
    else if(nextTiedGenes.size == 1) currentRank
    else{
      val sortedBySimScore = tiedGenes.toList.sortBy(sg=>sg.scoreFor(scoreIndex-4)).reverse.iterator
      currentRank - tiedGenes.size + simScoreRankGene(scoreIndex-4, sortedBySimScore, gene)
    }

  }else currentRank
}

def splitScoringLine(line:String) : (EntrezGene, (Double, Double, Double, Double)) = {
  val split = line.split(",")
  val gene = EntrezGeneSet.hpoAnnotatedGeneFor(split(0)).get

  val x1 = split(1).toDouble
  val x2 = split(2).toDouble
  val x3 = split(3).toDouble
  val x4 = split(4).toDouble
  (gene, (x1, x2, x3, x4))
}

def binSearch(a:Array[(Double, Double)], x:Double, low:Int = -1, high:Int = -1) : Either[(Double, Double), ((Double, Double), (Double,Double))] = {
  if(high == (-1) && low == (-1))binSearch(a, x, 0, a.length)
  else if(high < low || low > high)  Left((-1,-1)) //not found
  else{
    val mid = low + ((high - low) / 2)
    if(mid==0 && a(mid)._1>x)Left(a(mid))
    else if(mid==a.length-1 && a(mid)._1<x)Left(a(mid))
    else{
      if(a(mid)._1 > x){
        if(a(mid-1)._1 < x)Right(a(mid-1), a(mid))
        else binSearch(a, x, low, mid-1)
      }
      else if(a(mid)._1 < x){
        if(a(mid+1)._1 > x) Right(a(mid), a(mid+1))
        else binSearch(a, x, mid+1, high)
      }
      else Left(a(mid)) //exact match
    }
  }
}

def linFit(endpoints: Either[(Double, Double), ((Double, Double), (Double,Double))], x:Double) : Double = {
  endpoints match{
    case Left(tpl) =>{
      if(tpl._1==x)tpl._2 //exact match
      else if(tpl._1<x)1.0 //right endpoint of CDF
      else (tpl._2 / tpl._1) * x  //left endpoint of CDF
    }
    case Right(tpl) => {
      val m = (tpl._2._2-tpl._1._2)/(tpl._2._1-tpl._1._1)
      val b = tpl._2._2 - m * tpl._2._1
      m * x + b
    }
  }
}

def pvalFor(map:scala.collection.mutable.Map[Int,Array[(Double, Double)]], x:Double, gene:String, k:Int,
            sf:ontology.SimilarityFunction) : Double ={
  map.get(k) match {
    case Some(a) => 1.0 - linFit(binSearch(a, x), x)
    case _ => cdfTable(gene, k, sf) match {
      case Some(a) => {
        map.+=((k, a))
        1.0 - linFit(binSearch(a,x),x)
      }
      case _ => {
        log(s"ERROR: could not get CDFResult for k=$k, gene=$gene, sf=$sf")
        -1
      }
    }
  }
}

def diseaseDirFor(diseaseId: String, noise:Boolean , imprecision:Boolean) = {
  val prefix = s"./data/diseases/patients"
  if (noise && imprecision) s"$prefix/noiseImprecision/$diseaseId"
  else if (noise) s"$prefix/noise/$diseaseId"
  else if (imprecision) s"$prefix/imprecision/$diseaseId"
  else s"$prefix/optimal/$diseaseId"
}

def cdfTable(geneSym: String, k: Int, sf: ontology.SimilarityFunction): Option[Array[(Double, Double)]] = {
  ontology.entrezGeneBySymbol(geneSym) match{
    case Some(gene) => {
      val result = ontology.cdfResultFor(gene, sf, k)
      val cdf = new ArrayBuffer[(Double,Double)]()
      while(result.next){
        cdf.+=((result.getDouble(1), result.getDouble(2)))
      }
      Some(cdf.toArray.sortBy(t=>t._1))
    }
    case _ => None
  }
}

@tailrec
def toScalaList[T](iter: Iterator[T], l:List[T] = List[T]()) : List[T] = {
   if(iter.hasNext){
     val ll = iter.next() :: l
     toScalaList(iter, l)
   }else l
}

def addImprecision(terms:List[HPO_Term]) : List[HPO_Term] = {
  terms.map{term =>
    ontology.ancestors(term.id) match{
      case Some(s) => {
        val ancestors = s.filterNot(c=>c.key=="HP:0000001").toList
        if(ancestors.length>0)ancestors(Random.nextInt(ancestors.length)).value else term
      }
      case _ => {
        log(s"ERROR: No ancestors for term ${term.id}")
        term
      }
    }
  }
}

def diseaseEligibleNoiseTerms(disease:Disease) = {
  //this really should start with the terms annotated to the diseasae genes
  val diseaseTerms = disease.hpoFreqs.map{_.id}
  val nonEligibleNoise = (Set[String]() /: diseaseTerms){(s,id) =>
    val ancestors = {
      val s = ontology.ancestors(id).get
      if(s.isEmpty)Set[String]().empty else s.map{_.key}
    }
    val descendeants = {
      val s = ontology.descendants(id).get
      if(s.isEmpty)Set[String]().empty else s.map{_.key}
    }
    s.++(ancestors).++(descendeants)
  }
  ontology.annotatingConcepts.map{_.value}.filterNot(t=>nonEligibleNoise.contains(t.id)).toList
}


@tailrec
def addNoise(patientTerms:List[HPO_Term], eligibleNoiseTerms:List[HPO_Term], numNoiseTerms : Int = -1) : List[HPO_Term] = {
  if(numNoiseTerms < 0)addNoise(patientTerms, eligibleNoiseTerms, math.floor(patientTerms.length/2.0).toInt)
  else if(numNoiseTerms == 0) patientTerms
  else{
    val noiseTerm = eligibleNoiseTerms(Random.nextInt(eligibleNoiseTerms.length))
    if(patientTerms.contains(noiseTerm))addNoise(patientTerms, eligibleNoiseTerms, numNoiseTerms)
    else addNoise(noiseTerm :: patientTerms, eligibleNoiseTerms, numNoiseTerms-1)
  }
}

@tailrec
def diseasePatients(number:Int, disease: Disease, noise:Boolean = false, imprecision:Boolean = false,
             eligibleNoiseTerms : Option[List[HPO_Term]] = None, current:List[Patient] = List[Patient]()) : List[Patient] = {
  if(number==0)current
  else {
    val ent = eligibleNoiseTerms.getOrElse(diseaseEligibleNoiseTerms(disease))
    val patients = createPatient(disease, noise, imprecision, Some(ent)) :: current
    diseasePatients(number-1, disease, noise, imprecision, Some(ent), patients)
  }
}

@tailrec
def createOptimalPatients(number: Int, disease: Disease, current:List[Patient] = List[Patient]()) : List[Patient] = {
  if(number==0)current
  else createOptimalPatients(number-1, disease, createOptimalPatient(disease) :: current)
}

def createOptimalPatient(disease: Disease) : Patient = {
  val gender = if(Random.nextDouble()<0.5)Male else Female
  val patientTerms = disease.hpoFreqs.map{term =>
    val genderMatched = if(term.gender == GenderNA) true else gender == term.gender
    if(Random.nextDouble() < term.freq && genderMatched) Some(term.term) else None
  }.flatten
  if(patientTerms.isEmpty)createOptimalPatient(disease) //no asymptomatic patients
  else Patient(disease, patientTerms, gender)
}

def createPatient(disease: Disease, noise:Boolean = false, imprecision:Boolean = false, eligibleNoiseTerms : Option[List[HPO_Term]]) : Patient = {
  val gender = if(Random.nextDouble()<0.5)Male else Female
  val patientTerms = disease.hpoFreqs.map{term =>
    val genderMatched = if(term.gender == GenderNA) true else gender == term.gender
    if(Random.nextDouble() < term.freq && genderMatched) Some(term.term) else None
  }.flatten
  if(patientTerms.isEmpty)createPatient(disease, noise, imprecision, eligibleNoiseTerms) //no asymptomatic patients
  else {
    if(noise && imprecision)Patient(disease, addNoise(addImprecision(patientTerms), eligibleNoiseTerms.getOrElse(diseaseEligibleNoiseTerms(disease))), gender)
    else if(noise) Patient(disease, addNoise(patientTerms, eligibleNoiseTerms.getOrElse(diseaseEligibleNoiseTerms(disease))), gender)
    else if(imprecision)Patient(disease, addImprecision(patientTerms), gender)
    else Patient(disease, patientTerms, gender)
  }
}

def toJson(omim:OMIMEntry, hpoFreqs: List[HPOFreq]) = {
  s"""{${omim.toJson}, "HPO_FREQ":[${(""/:hpoFreqs){(s,h)=> s+h.toJson+","}.dropRight(1)}]}"""
}

/**
 * @param omim
 * @param termID
 */
def recordDroppedTerm(omim: OMIMEntry, termID: String) : Unit = {
   val f = s"./data/diseases/droppedTerms/${omim.id}_dropped_HPOTerms.txt"
  appendToFile(f){pw => pw.println(s"$termID")}
}

def extractHpoFreqUnchecked(entry: Map[String, Any], omim: OMIMEntry) : Either[String, HPOFreq] = {
  val id = entry.get("term").get.toString
  val freq = entry.get("freq").get.toString
  val gender = entry.get("gender").get.toString match{
    case "M" => Male
    case "F" => Female
    case _ => GenderNA
  }

  HPO_Term.getConceptById(id) match {
    case Some(c) =>  Right(HPOFreq(id, freq.toDouble, gender))
    case _ =>  Left(id)
  }

}

def extractHpoFreq(entry: Map[String, Any], omim: OMIMEntry, annotations: Set[Concept[String, HPO_Term]]) : Either[String, HPOFreq] = {
  val id = entry.get("term").get.toString
  val freq = entry.get("freq").get.toString
  val gender = entry.get("gender").get.toString match{
    case "M" => Male
    case "F" => Female
    case _ => GenderNA
  }

  HPO_Term.getConceptById(id) match {
    case Some(c) => if (annotations.contains(c)) Right(HPOFreq(id, freq.toDouble, gender)) else Left(id)
    case _ =>  Left(id)
  }

}

def extractOmim(m:Map[String,Any]) = {
  m.get("OMIM") match{
    case Some(om:Map[String,Any]) => {
      val id = om.get("id").get.toString
      val name = om.get("name").get.toString
      val genes = om.get("genes").get.asInstanceOf[List[String]]
      Some(OMIMEntry(id, name, genes))
    }
    case _ => None
  }
}


//--------------------------------------------------------------------------------
//UTILITY DEFS
//--------------------------------------------------------------------------------
def printToFile(path: String)(op: PrintWriter => Unit){
  val f = new File(path)
  if(f.exists)f.delete
  val pw = new PrintWriter(new File(path))
  try {op(pw)} finally { pw.close() }
}

def appendToFile(path: String)(op: PrintWriter => Unit){
  val f = new File(path)
  if(!f.getParentFile.exists())f.getParentFile.mkdirs()
  val pw = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))
  try {{op(pw)}} finally {pw.close()}
}

def log(msg:String)={
  val path = "./log.txt"
  appendToFile(path){pw => pw.println(msg)}
}

