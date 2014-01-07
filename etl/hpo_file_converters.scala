import java.io.{FileWriter, BufferedWriter, File, PrintWriter}
import scala.annotation.tailrec
import scala.io.Source
import scala.util.matching.Regex
import java.io.File

//*****READ THIS*********
//The following variables must be updated to use a new versions of the HPO ontology and Gene to Phenotype mapping files
//INPUT FILES
val root = "/Users/masinoa/chop/dev/production/clinseq_U01/varprior/phenomantics/etl/hpo_ingest"
val hpo_path = "%s/data/hp_860_131217.obo".format(root)
val entrez_hpo_path = "%s/data/genes_to_phenotype_26_130201.txt".format(root)
val expected_number_annotations = 61784
val disease_hpo_path = "%s/data/disease_to_phenotype_365_131217.txt".format(root)
val expected_number_disease_annotations = 111565

//OUTPUT FILES
val hpo_terms_path = "%s/output/HPO_TERMS.txt".format(root) 
val hpo_alt_ids_path = "%s/output/HPO_ALT_IDS.txt".format(root)
val hpo_ancestors_path = "%s/output/HPO_ANCESTORS.txt".format(root)
val entrez_hpo_annotations_path = "%s/output/ENTREZ_HPO_ANNOTATIONS.txt".format(root)
val entrez_path = "%s/output/ENTREZ.txt".format(root)
val disease_path = "%s/output/DISEASES.txt".format(root)
val disease_hpo_annotations_path = "%s/output/DISEASES_HPO_ANNOTATIONS.txt".format(root)
List(entrez_hpo_annotations_path, entrez_path, hpo_terms_path, hpo_alt_ids_path, hpo_ancestors_path) foreach {path => 
	val f = new File(path)
    if(f.exists)f.delete
}

def read_lines(path:String) = {
  Source.fromFile(path).getLines()
}
//REGEX PATTERNS
val PROP_REGX = """^(\w+)?:\s*(.+)?""".r
val ID_REGX = """id:\s*HP:(\d+)?""".r

// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EXTRACT THE HPO TERMS FILE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
/* convenience class for storing current state as HP.obo file is traversed */
case class hpoPackage(terms: List[(String,String)], alt_ids: List[(String, String)], ancestors: List[(String,String)], id_box: Option[String])

val hpo = (hpoPackage(List[(String,String)](),  List[(String,String)](),  List[(String,String)](), None)  /: read_lines(hpo_path)) { (pack, line) =>
		pack.id_box match{
				case Some(id) => {
					line.trim match{
						case "" => pack
						case "[Term]" => hpoPackage(pack.terms, pack.alt_ids, pack.ancestors, None)
						case _ => {
							val PROP_REGX(tag, value) = line.trim
							tag.trim match {
								case "name" => hpoPackage(id->value.trim :: pack.terms, pack.alt_ids, pack.ancestors, Some(id))
								case "alt_id" => hpoPackage(pack.terms, value.trim->id :: pack.alt_ids, pack.ancestors, Some(id))
								case "is_a" => hpoPackage(pack.terms, pack.alt_ids, id->value.split("!")(0).trim :: pack.ancestors, Some(id))
								case _ => pack
							}
						}
					}
				}
				case _ => {
					ID_REGX findFirstIn line match{
						case Some(id_line) => {
							val ID_REGX(id) = id_line
							hpoPackage(pack.terms, pack.alt_ids, pack.ancestors, Some("HP:%s".format(id.trim)))
						}
						case _ => pack
					}
				}
		  }//end id_box match
		}//end fold left

val alt_id_map = hpo.alt_ids.toMap
val hpo_ids = hpo.terms map { term => term._1}

// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EXTRACT HPO TO GENE ANNOTATIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
case class EntrezPackage(hpo_annotations : Map[String,Set[String]] = Map[String,Set[String]](), id_name : Map[String, String] = Map[String,String]())
//case class EntrezPackage(hpo_annotations : Map[String,Set[String]] , id_name : Map[String, String] )

val ID_FROM_LINE_REGX = """HP:(\d+)?""".r
val LINE_REGX = """(\d+)?\s+([\w\d-@]+)?\s+(.*)?""".r
val entrez_hpo_lines = read_lines(entrez_hpo_path)
entrez_hpo_lines.next //skip first header line
var counter = 0
val entrez_hpo = (EntrezPackage() /: entrez_hpo_lines) { (ep, line) =>
			val LINE_REGX(entrez_id, entrez_name, mapped_hpo_terms) = line.trim
			val id_name = ep.id_name + (entrez_id -> entrez_name)
			val hpo_annotations = (ep.hpo_annotations /: (ID_FROM_LINE_REGX findAllIn mapped_hpo_terms)){ (m, given_id) =>
				val proper_id = if(hpo_ids.contains(given_id))Some(given_id)
								else alt_id_map.get(given_id)

				proper_id match {
					case Some(hpo_id) => {
						m.get(entrez_id) match{
							case Some(s) => {
                                if(s.contains(hpo_id))counter = counter +1 //some alt ids and proper ids are both mapped to the same gene
                                m + (entrez_id -> (s + hpo_id))
                            }
							case _ => m + (entrez_id -> Set(hpo_id))
						}
					}
					case _ => {
						println("WARNING: No proper HPO ID found for %s from %s".format(given_id,entrez_hpo_path))
					    m
					}
				}
		}// end hpo_annotations fold left
	EntrezPackage(hpo_annotations, id_name)
} //end Some(lines) fold left

//sanity checks
println(s"counter = $counter")
val x = (0 /: entrez_hpo.hpo_annotations.values){(count,s) => count + s.size}
if(x==(expected_number_annotations-counter))println("Message: Total Gene Annotations Equals Expected")
else println("WARNING: Total Gene Annotations, %d, did not equal expected Gene Annotations, %d".format(x,expected_number_annotations-counter))

// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EXTRACT THE HPO TO DISEASE ANNOTATIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
case class DiseasePackage(hpo_annotations : Map[String,Set[String]] = Map[String,Set[String]](), id_name : Map[String, String] = Map[String,String]())
val disease_lines = read_lines(disease_hpo_path)
counter = 0
var lineCount = 0
val disease_hpo = (DiseasePackage() /: disease_lines){(dp, line) =>
  val entries = line.replace("\t\t\t\t","\t").replace("\t\t","\t").split("\t")
  val diseaseId = s"${entries(0)}:${entries(1)}"
  val id_name = dp.id_name + (diseaseId->entries(2))
  val givenHpoId = if(entries(3).startsWith("HP:")) entries(3) else entries(4)
  val proper_id = if(hpo_ids.contains(givenHpoId)) Some(givenHpoId) else alt_id_map.get(givenHpoId)
  lineCount = lineCount + 1
  proper_id match{

    case Some(hpo_id) => {
      dp.hpo_annotations.get(diseaseId) match{
        case Some(s) => {
          if(s.contains(hpo_id))counter = counter +1 //some alt ids and proper ids are both mapped to the same disease
          DiseasePackage(dp.hpo_annotations + (diseaseId->(s+hpo_id)), id_name)
        }
        case _ => DiseasePackage(dp.hpo_annotations + (diseaseId->Set(hpo_id)), id_name)
      }
    }
    case _ => {
      println(s"WARNING: No proper HPO ID found for $givenHpoId on line $lineCount")
      dp
    }
  }
}

//sanity checks
println(s"counter = $counter")
val ds = (0 /: disease_hpo.hpo_annotations.values){(count,s) => count + s.size}
if(ds==(expected_number_disease_annotations - counter))println("Message: Total Disease Annotations Equals Expected")
else println("WARNING: Total Disease Annotations, %d, did not equal expected disease Annotations, %d".format(ds,expected_number_disease_annotations - counter))
// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% WRITE OUTPUT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
//Write hpo terms to output file
printToFile(hpo_terms_path){pw =>
  hpo.terms.sortBy{tpl=>tpl._1}.map{tpl => "%s\t%s".format(tpl._1,tpl._2)}.foreach{s=>pw.println(s)}
}

//Write ancestors to output file
val ancestors_map = (Map[String,Set[String]]() /: hpo.ancestors){ (m, tpl) => 
	if(m.contains(tpl._1)) m + (tpl._1 -> (m(tpl._1) + tpl._2))
    else m + (tpl._1 -> Set(tpl._2))
}
printToFile(hpo_ancestors_path){pw =>
	ancestors_map.foreach{tpl =>
		pw.println("%s\t%s".format(tpl._1, (tpl._2.head /: tpl._2.tail){_+","+_}))
	}
}

//Write alt ids to output file
printToFile(hpo_alt_ids_path){pw =>
   hpo.alt_ids.sortBy{tpl=>tpl._1}.map{tpl => "%s\t%s".format(tpl._1,tpl._2)}.foreach{s=>pw.println(s)}
}

//Write entrez gene id to entrez name file
printToFile(entrez_path){pw =>
  entrez_hpo.id_name.toList.sortBy{tpl=>tpl._1}.foreach{tpl => pw.println("%s\t%s".format(tpl._1,tpl._2))}
}

//Write  entrez gene id to hpo id association file
printToFile(entrez_hpo_annotations_path){pw =>
	entrez_hpo.hpo_annotations.toList.sortBy{tpl=>tpl._1}.foreach{tpl => pw.println("%s\t%s".format(tpl._1, (tpl._2.head /: tpl._2.tail){_+","+_}))}
}

//Write disease id to disease name file
printToFile(disease_path){pw =>
  disease_hpo.id_name.toList.sortBy{tpl=>tpl._1}.foreach{ tpl =>
    val split = tpl._1.split(":")
    pw.println(s"${split(0)}\t${split(1)}\t${tpl._2}")
  }
}

//Write disease id to hpo id association file
printToFile(disease_hpo_annotations_path){pw =>
  disease_hpo.hpo_annotations.toList.sortBy{tpl=>tpl._1}.foreach{ tpl =>
    val split = tpl._1.split(":")
    pw.println("%s\t%s\t%s".format(split(0), split(1), (tpl._2.head /: tpl._2.tail){_+","+_}))
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

println("ALL DONE")
