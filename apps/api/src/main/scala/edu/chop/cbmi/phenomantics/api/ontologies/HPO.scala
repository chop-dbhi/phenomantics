package edu.chop.cbmi.phenomantics.api.ontologies

import edu.chop.cbmi.phenomantics.api.gene.{EntrezGeneSet, EntrezGene}
import io.Source
import scala.language.postfixOps
import com.typesafe.config.ConfigFactory
import java.sql.{ResultSet, DriverManager}
import java.io.File

case class HPO_Term(term: String, id: String, alt_id: Set[String] = Set.empty[String])

object HPO_Term extends{

  /**
   * all HPO concepts as type HPO_Term
   */
  lazy val allTerms: Set[HPO_Term] = {
    val initialSet: Set[HPO_Term] = (Source.fromURL(getClass.getResource("/HPO_TERMS.txt")).getLines map {(line:String) =>
      line.split("\t") match{
        case a:Array[String] => if (a.length==2)Some(HPO_Term(a(1), a(0))) else None
        case _ => None
      }
    }).flatten.toSet

    val altIdsLines = Source.fromURL(getClass.getResource("/HPO_ALT_IDS.txt")).getLines

    (initialSet /: altIdsLines) { (s, line) =>
      line.split("\t") match{
        case a:Array[String] => if (a.length==2){
          val altID = a(0)
          val trueID = a(1)
          s.find(_.id==trueID) match{
            case Some(hpo) => s - hpo + HPO_Term(hpo.term, hpo.id, hpo.alt_id + altID)
            case _ => s
          }
        } else s
        case _ => s
      }
    }
  }

  def getTermById(id: String) = allTerms.find(_.id==id)

  def getTermByName(name: String) = allTerms.find(_.term==name)

  /**
   * all HPO concepts as type Concept[String, HPO_Term]
   */
  lazy val allConcepts : Set[Concept[String, HPO_Term]] = {

    val initial = allTerms map { t => Concept(t.id, t) -> Set.empty[Concept[String,HPO_Term]]} toMap

    val lines: Iterator[String] = Source.fromURL(getClass.getResource("/HPO_ANCESTORS.txt")).getLines

    val parentConceptMap : Map[Concept[String, HPO_Term], Set[Concept[String, HPO_Term]]] =
      (initial /: lines){ (m,line) =>
      line.split("\t") match{
        case a:Array[String] => if (a.length==2){
          val child_key = a(0)
          val parent_keys = a(1).split(",").toSet
          initial.find(_._1.key==child_key) match{
            case Some(tpl) => {
              val concept = tpl._1
              val parents = (tpl._2 /: parent_keys){(s,pk) =>
                initial.find(_._1.key==pk) match{
                  case Some(ptpl) => s + ptpl._1
                  case _ => s
                }
              }
              m + (concept->parents)
            }
            case _ => initial
          }
        } else initial
        case _ => initial
      }
    }

    Concept(parentConceptMap)
  }

  def getConceptById(id: String) = allConcepts.find(_.key==id)

  def getConceptByName(name: String) = allConcepts.find(_.value.term==name)
}

class HPOEntrezAnnotatingOntology private[ontologies] (annotations: Map[EntrezGene, Set[Concept[String, HPO_Term]]],
                                  concepts: Set[Concept[String, HPO_Term]])
  extends AnnotatingOntology[EntrezGene, String, HPO_Term](annotations, concepts) with Similarity[EntrezGene, String, HPO_Term]{

  def hpoConceptByName(name: String) = concepts.find(_.value.term==name)

  def hpoConceptById(id: String) = concepts.find(_.key==id)

  def entrezGeneById(id: Int) : Option[EntrezGene] = annotations.keys.find(_.id==id)

  def entrezGeneBySymbol(symbol: String) : Option[EntrezGene] = annotations.keys.find(_.symbol==symbol)

  /**
   * term distance
   * @param c1
   * @param c2
   * @return
   */
  def distanceSim2(c1: Concept[String, HPO_Term], c2: Concept[String, HPO_Term]) : Double = {
    if(c1==c2)0.0
    else -1.0 * math.log(sim2(c1,c2))
  }

  /**
   * term set distance
   * @param s1
   * @param s2
   * @return
   */
  def symDistanceSim2(s1: Set[Concept[String, HPO_Term]], s2: Set[Concept[String, HPO_Term]]) : Double = {
    val Ds1s2 = ((0.0 /: s1){ (csum, c1) =>
      csum + (Double.MaxValue /: s2){ (cmin, c2) => math.min(cmin, distanceSim2(c1,c2))}
    })/s1.size.toDouble

    val Ds2s1 = ((0.0 /: s2){ (csum, c2) =>
      csum + (Double.MaxValue /: s1){ (cmin, c1) => math.min(cmin, distanceSim2(c1,c2))}
    })/s2.size.toDouble

    (Ds1s2 + Ds2s1) * 0.5
  }

  trait SimilarityFunction
  object AsymMaxSim1 extends SimilarityFunction
  object SymMaxSim1 extends SimilarityFunction
  object SymMaxSim2 extends SimilarityFunction
  object AsymMaxSim2 extends SimilarityFunction
  lazy private val simFuncLabels = ConfigFactory.load.getConfig("phenomanticsApi.db.simFuncLabels")
  lazy private val dbTablesMaxK = ConfigFactory.load.getConfig("phenomanticsApi.db").getInt("maxK")

  private def tableNameFor(template: String, k : Int) = {
    val l = template.lastIndexOf("#") - template.indexOf("#") + 1
    val ks = k.toString
    template.replaceAll("#+", s"${"0"*(l-ks.length)}$ks")
  }

  /**
   * obtains result set from db tables, two columns: score, cdf
   * NOTE: use of this function requires the existence of a database with the CDF values and
   * a credentials.conf file located in ./conf/credentials.conf
   * @param gene
   * @param sf
   * @param k
   * @return
   */
  def cdfResultFor(gene : EntrezGene, sf: SimilarityFunction, k: Int): ResultSet = {
    val kn = math.min(k,dbTablesMaxK)
    val tableName = sf match{
      case AsymMaxSim1 => tableNameFor(simFuncLabels.getString("AsymMaxSim1"),kn)
      case SymMaxSim1 => tableNameFor(simFuncLabels.getString("SymMaxSim1"),kn)
      case SymMaxSim2 => tableNameFor(simFuncLabels.getString("SymMaxSim2"),kn)
      case AsymMaxSim2 => tableNameFor(simFuncLabels.getString("AsymMaxSim2"),kn)
    }
    val qi = DBBackend.quoteIdentifier
    val qry = s"SELECT score,cdf FROM $qi$tableName$qi WHERE gene_id=${gene.id};"
    val db = DBBackend.db
    val sql = db.createStatement()
    sql.setFetchSize(100)
    sql.executeQuery(qry)
  }

  /**
   * Computes the pvalue for a given score given a sql result set with two columns: score, cdf
   * @param score
   * @param resultSet
   * @return
   */
  def pvalue(score : Double, resultSet : ResultSet) = {
    var lcdf = 0.0
    var rcdf = 1.0
    var lscore = 0.0
    var rscore = Double.MaxValue
    //NOTE CANNOT ASSUME THE RESULT SET IS SORTED SO NEED TO CHECK ALL VALUES
    while(resultSet.next()){
      val row_score = resultSet.getDouble(1)
      val cdf = resultSet.getDouble(2)
      //check left boundary
      if(row_score<=score && row_score>=lscore){
        lscore = row_score
        lcdf = cdf
      }
      //check right boundary
      if(row_score>=score && row_score<=rscore){
        rscore = row_score
        rcdf = cdf
      }
    }
    if(lscore == -1 && rscore == -1){
      //implies result set is empty
      throw new Exception(s"Cannot compute p-value with empty result set")
    } else if(lscore == -1) 1.0-rcdf //have no lcdf to interpolate with
    else if(rscore == -1) 1.0-lcdf //have no rcdf to interpolate with
    else if(lscore==rscore)1.0-rcdf //have exact match
    else{ //perform linear interpolation between cdf values
      val m = (rcdf-lcdf)/(rscore-lscore)
      val b = rcdf - m * rscore
      1.0 - (m * score + b)
    }
  }

  /**
   * computes the pvalue for a given score given an iterable where the items in the iterable are
   * 2tuples with first value score, second value cdf
   * @param score
   * @param cdf
   * @return
   */
  def pvalue(score : Double, cdf : Iterable[(Double,Double)]) = {
    if(cdf.isEmpty)throw new Exception(s"Cannot compute p-value with empty result set")
    val (lcdf, rcdf, lscore, rscore) = ((0.0, 1.0, 0.0, Double.MaxValue) /: cdf){(tpl,row) =>
       val row_score = row._1
       val row_cdf = row._2
       val clcdf = tpl._1
       val crcdf = tpl._2
       val clscore = tpl._3
       val crscore = tpl._4

       if(row_score<=score && row_score>=clscore)(row_cdf, crcdf, row_score, crscore)
       else if(row_score>=score && row_score<=crscore)(clcdf, row_cdf, clscore, row_score)
       else(clcdf, crcdf, clscore, crscore)
    }
    if(lscore == -1 && rscore == -1){
      //implies result set is empty
      throw new Exception(s"Cannot compute p-value with empty result set")
    } else if(lscore == -1) 1.0-rcdf //have no lcdf to interpolate with
    else if(rscore == -1) 1.0-lcdf //have no rcdf to interpolate with
    else if(lscore==rscore)1.0-rcdf //exact match
    else{ //perform linear interpolation between cdf values
      val m = (rcdf-lcdf)/(rscore-lscore)
      val b = rcdf - m * rscore
      1.0 - (m * score + b)
    }
  }

  /**
   * computes the similarity between the terms in query and those directly annotating gene
   * computes the p-value for the similarity score from db tables
   * NOTE: use of this function requires the existence of a database with the CDF values and
   * a credentials.conf file located in ./conf/credentials.conf
   * @param gene
   * @param query
   * @param sf
   * @return (pvalue, similarity)
   */
  def pvalue(gene: EntrezGene, query: Set[HPO_Term], sf: SimilarityFunction) : (Double, Double) = {
    annotationsFor(gene) match{
      case Some(geneConcepts) => {
        val qryConcepts = query.map{term=> hpoConceptById(term.id)}.flatten
        val k = math.min(qryConcepts.size, dbTablesMaxK)
        if(k==0) throw new Exception("The query set must contain a valid HPO Concept")
        else{
          val score = sf match{
            case AsymMaxSim1 =>asymMaxSim1(qryConcepts, geneConcepts)
            case SymMaxSim1 => symMaxSim1(qryConcepts,geneConcepts)
            case SymMaxSim2 => symMaxSim2(qryConcepts,geneConcepts)
            case AsymMaxSim2 => asymMaxSim2(qryConcepts, geneConcepts)
            case _ => throw new Exception("Unknown Similarity function")
          }
          (pvalue(score,cdfResultFor(gene, sf, k)), score)
        }
      }
      case _ => throw new Exception(s"Gene ${gene.symbol} has no annotations and cannot be scored")
    }
  }

}

object DBBackend{
  val db = {
    val credsFile = new File("./conf/credentials.conf")
    val creds = ConfigFactory.parseFile(credsFile).getConfig("db.credentials")
    Class.forName(creds.getString("driverClassName"))
    DriverManager.getConnection(creds.getString("jdbcUri"), creds.getString("user"), creds.getString("password"))
  }


  val quoteIdentifier = {
    val credsFile = new File("./conf/credentials.conf")
    val db = ConfigFactory.parseFile(credsFile).getConfig("db")
    db.getString("quoteIdentifier")
  }
}

object HPOEntrezFactory{

  lazy private val entrez = EntrezGeneSet.hpo_annotated_genes

  lazy private val concepts = HPO_Term.allConcepts

  lazy private val annotations: Map[EntrezGene, Set[Concept[String, HPO_Term]]] =
    (Source.fromURL(getClass.getResource("/ENTREZ_HPO_ANNOTATIONS.txt")).getLines map {
      (line: String) =>
        line.split("\t") match {
          case a: Array[String] => if (a.length == 2) {
            entrez.find(_.id == a(0).toInt) match {
              case Some(gene) => {
                val as = a(1).split(",")
                Some(gene -> ((Set.empty[Concept[String, HPO_Term]] /: as) { (s, k) =>
                  concepts.find( (c) => c.key == k || c.value.alt_id.contains(k)) match {
                    case Some(c) => s + c
                    case _ => s
                  }
                }))
              }
              case _ => None
            }
          } else None
          case _ => None
        }
    }).flatten.toMap

   lazy val informationAO = new HPOEntrezAnnotatingOntology(annotations, concepts)

}