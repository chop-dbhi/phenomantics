package edu.chop.cbmi.phenomantics.api.gene
import scala.io.Source

case class EntrezGene(id: Int, symbol: String) extends Ordered[EntrezGene] {
  def compare(that: EntrezGene) = this.id.compare(that.id)
}

object EntrezGeneSet{
  
  lazy val hpo_annotated_genes = {
    (Source.fromURL(getClass.getResource("/ENTREZ.txt")).getLines map {(line:String) => 
    	line.split("\t") match{
    	  case a:Array[String] => if(a.length==2)Some(EntrezGene(a(0).toInt, a(1))) else None
    	  case _ => None
    	}
    }).flatten.toSet //flatten removes None items
  }

  lazy val hpo_annotated_genes_sorted_id = hpo_annotated_genes.toList.sortBy(_.id)
  
  def hpoAnnotatedGeneFor(symbol: String) : Option[EntrezGene] = hpo_annotated_genes.find(_.symbol==symbol)
}