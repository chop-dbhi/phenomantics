import edu.chop.cbmi.phenomantics.api.gene.EntrezGene
import edu.chop.cbmi.phenomantics.api.ontologies._
import java.io.{FileWriter, BufferedWriter, File, PrintWriter}
import scala.language.postfixOps

val ontology = HPOEntrezFactory.informationAO

val genes: Iterable[EntrezGene] = ontology.itemsAnnotatedAll

val dataFile = "../data/Scenario_1_a_Ranks.txt"
val errorFile = "../data/Scenario_1_a_errors.txt"


//scenario 1 a
appendToFile(dataFile) {
  p => p.println("E_ID\tE_Symbol\tSim_Score\tPosition")
}

genes foreach {
  gene =>
    ontology.annotationsFor(gene) match {
      case Some(terms) => {

        val similaritiespar = (genes.par map {
          g =>
            ontology.annotationsFor(g) match {
              case Some(gTerms) => {
                Some(g -> ontology.symMaxSim2(terms, gTerms))
              }
              case _ => None
            }
        })

        val similarities : List[(EntrezGene, Double)] = similaritiespar.flatten.toList.sortWith((l, r) => l._2 > r._2)

        val (position, sim, found) = ((0, -1.0, false) /: similarities) {
          (x, ed) =>
            val (p, s, f) = x
            val ts = ed._2
            if (f) {
              if (s == ts) (p + 1, s, f)
              else x
            } else (p+1, ts, ed._1.id == gene.id)
        }


        if (found) appendToFile(dataFile) {
          p => p.println("%d\t%s\t%f\t%d".format(gene.id, gene.symbol, sim, position))
        }
        else appendToFile(errorFile) {
          p => p.println("%s: No rank for".format(gene.symbol))
        }
      }
      case _ => appendToFile(errorFile) {
        p => p.println("%s: No annotations".format(gene.symbol))
      }
    }
}
//end scenario 1 a

//--------------------------------------------------------------------------------
//UTILITY DEFS
//--------------------------------------------------------------------------------
def printToFile(path: String)(op: PrintWriter => Unit) {
  val f = new File(path)
  if (f.exists) f.delete
  val pw = new PrintWriter(new File(path))
  try {
    op(pw)
  } finally {
    pw.close()
  }
}

def appendToFile(path: String)(op: PrintWriter => Unit) {
  val pw = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))
  try { {
    op(pw)
  }
  } finally {
    pw.close()
  }
}

println("Done")
