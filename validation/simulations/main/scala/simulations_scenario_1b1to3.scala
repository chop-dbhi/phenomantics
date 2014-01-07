import edu.chop.cbmi.phenomantics.api.gene.EntrezGene
import edu.chop.cbmi.phenomantics.api.ontologies._
import java.io.{FileWriter, BufferedWriter, File, PrintWriter}
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

type HPO = Concept[String, HPO_Term]
val ontology = HPOEntrezFactory.informationAO

case class RankData(gene:EntrezGene, simScoreFound: Boolean, simScore: Double, simScorePosition: Int,
                    pvalueFound: Boolean, pvalue: Double, pvaluePositon: Int) {
  lazy val numTerms = ontology.annotationsFor(gene) match{
    case Some(s) => s.size
    case _ => -1
  }
  def toLine() = s"${gene.id},${gene.symbol},$numTerms,$simScore,$simScorePosition,$pvalue,$pvaluePositon"
}

val genes: Iterable[EntrezGene] = ontology.itemsAnnotatedAll
val simFunc = ontology.AsymMaxSim1
val simFunction = ontology.asymMaxSim1 _
val k = 1

//Create map from gene->most, median, least informative term called GeneTerms
val geneTerms = (Map[EntrezGene,(HPO, HPO, HPO)]()/:genes){(map,gene) =>
  ontology.annotationsFor(gene) match{
    case Some(allTerms) => {
      //get the HPO terms associated with this gene and sort them in ascending order by information content
      val ic = (allTerms map { t => t->ontology.information_content(t)}).toList.sortWith {(l,r) => l._2 < r._2}
      val mostInform = ic(ic.length-1)._1
      val leastInform = ic(0)._1
      val medianInform = ic((ic.length/2.0).floor.toInt)._1
      map + ((gene, (mostInform, leastInform, medianInform)))
    }
    case _ => map
  }
}

//Create flat SET of terms from the map
val termsToScore = geneTerms.values.par.map{tpl=>Set(tpl._1, tpl._2, tpl._3)}.flatten.toSet

//Create map from gene->Map(term,score,pvalue)
val geneScores = (Map[EntrezGene, Map[HPO, (Double, Double)]]() /: genes){(m1, gene) =>
  ontology.annotationsFor(gene) match{
    case Some(geneAnnotations) => {
      //pull the cdf table into memory so we only have to go to the db once
      val result = ontology.cdfResultFor(gene, simFunc, k)
      val cdf = new ArrayBuffer[(Double,Double)]()
      while(result.next){
        cdf.+=((result.getDouble(1), result.getDouble(2)))
      }
      (m1 /: termsToScore){(m2, term) =>
        val qry = Set(term)
        val simScore = simFunction(qry,geneAnnotations)
        val pval = ontology.pvalue(simScore, cdf)
        m2.get(gene) match{
          case Some(scoreMap) => m2 + ((gene, scoreMap + ((term,(pval,simScore)))))
          case _ => m2 + ((gene, Map(term->(pval,simScore))))
        }
      } //fold left over termsToScore
    } //end case Some(geneAnnotations)

    case _ => m1 //can't score a gene without annotations
  } //end ontology.annotationsFor(gene) match
}

val dataFiles = List("1","2","3") map { i => "./data/Scenario_1_b_%s_Ranks.txt".format(i)}
val errorFile ="./data/Scenario_1_b_1-3_errors.txt"

dataFiles foreach { f => appendToFile(f){p => p.println("E_ID,E_Symbol,Num_Annotations,Sim_Score,SimScore_Position,P_Value,P_Value_Position")}}

def computeRanks(gene:EntrezGene, infoIdx: Int) = {
   geneTerms.get(gene) match{
     case Some(tpl) => {
       val term = tpl.productElement(infoIdx).asInstanceOf[HPO]
       val (sortedSimScores, sortedPValues) = {
         val (sss, spv) = ((List[(EntrezGene,Double)](), List[(EntrezGene,Double)]()) /: geneScores){(lists, tpl) =>
           tpl._2.get(term) match{
             case Some(scoreTpl) => {
               val scoreList = lists._1
               val pvalueList = lists._2
               val nextGene = tpl._1
               ((nextGene,scoreTpl._2)::scoreList, (nextGene, scoreTpl._1)::pvalueList)
             }
             case _ => lists
           }
         }
         (sss.sortWith((l,r) =>l._2 > r._2), spv.sortWith((l,r) =>l._2 < r._2))
       }

       val (simScorePos, simScore, simScoreFound) = ((0, -1.0, false) /: sortedSimScores) {
         (x, ed) =>
           val (p, s, f) = x
           val ts = ed._2
           if (f) {
             if (s == ts) (p + 1, s, f)
             else x
           } else (p+1, ts, ed._1.id == gene.id)
       }

       val (pValuePos, pvalue, pValFound) = ((0, -1.0, false) /: sortedPValues) {
         (x, ed) =>
           val (p, s, f) = x
           val ts = ed._2
           if (f) {
             if (s == ts) (p + 1, s, f)
             else x
           } else (p+1, ts, ed._1.id == gene.id)
       }
       RankData(gene, simScoreFound, simScore, simScorePos, pValFound, pvalue, pValuePos)
     }
     case _ => throw new Exception("can't rank term not in geneTerms")
   }
}

for(i <- 0 until 3){
  val ranks = genes.par.map{gene =>
    computeRanks(gene, i)
  }
  appendToFile(dataFiles(i)){p =>
    ranks.foreach{rank=>p.println(rank.toLine())}
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
  val pw = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))
  try {{op(pw)}} finally {pw.close()}
}

println("Done")
