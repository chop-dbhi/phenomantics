/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 2/7/13
 * Time: 2:21 PM
 * To change this template use File | Settings | File Templates.
 */

import edu.chop.cbmi.phenomantics.api.gene.EntrezGene
import edu.chop.cbmi.phenomantics.api.ontologies._
import java.io.{File, PrintWriter}

println("Starting ...")

val ontology = HPOEntrezFactory.informationAO

//--------------------------------------------------------------------------------
//gather data for analysis of number of genes with k direct annotations
//--------------------------------------------------------------------------------
val entrezDirectCount: Map[EntrezGene, Int] = (Map[EntrezGene, Int]() /: ontology.values) { (m,c) =>
  ontology.itemsAnnotatedDirectlyBy(c) match{
    case Some(s) =>  (m /: s) { (nm, eg) =>
      nm.get(eg) match{
        case Some(i) => nm + (eg->(i+1))
        case _ => nm + (eg->1)
      }
    }
    case _ => m
  }
}

val kToNumDirectGenes = (Map[Int, Int]() /: entrezDirectCount.values) {(m, k) =>
  m.get(k) match{
    case Some(c) => m + (k->(c+1))
    case _ => m + (k->1)
  }
}

//quick sanity check
val n = (0 /: kToNumDirectGenes) { (c, t) => c + t._1 * t._2}
val nd = (0 /: ontology.values) { (cnt, concept) =>
  ontology.countDirectFor(concept) match{
    case Some(i) => cnt + i
    case _ => cnt
  }
}
if(n!=nd)println("Warning: The sum over k of the number of genes with k annotations * k = %d does not equal ontology.countAll = %d".format(n, nd))

printToFile("../data/num_genes_K_direct_annotations.txt"){ p =>
  ((kToNumDirectGenes.toList.sortWith((t1,t2)=> t1._1<t2._1)).::(("k"->"num_genes"))) map { tpl => tpl._1 + "," + tpl._2} foreach {p.println}
}

//--------------------------------------------------------------------------------
// gather data for analysis of number of genes with k indirect annotations
//--------------------------------------------------------------------------------
val entrezIndirectCount = (Map[EntrezGene, Int]() /: ontology.values) { (m,c) =>
  ontology.itemsAnnotatedDescendantBy(c) match{
    case Some(s) =>  (m /: s) { (nm, eg) =>
      nm.get(eg) match{
        case Some(i) => nm + (eg->(i+1))
        case _ => nm + (eg->1)
      }
    }
    case _ => m
  }
}

val kToNumIndirectGenes = (Map[Int, Int]() /: entrezIndirectCount.values) {(m, k) =>
  m.get(k) match{
    case Some(c) => m + (k->(c+1))
    case _ => m + (k->1)
  }
}

//quick sanity check
val i = (0 /: kToNumIndirectGenes) { (c, t) => c + t._1 * t._2}
val ic = (0 /: ontology.values) { (cnt, concept) =>
  ontology.countDescendantFor(concept) match{
    case Some(i) => cnt + i
    case _ => cnt
  }
}
if(i!=ic)println("Warning: The sum over k of the number of genes with k indirect annotations * k = %d does not equal sum of concept indirect counts = %d".format(i,ic))

printToFile("../data/num_genes_K_indirect_annotations.txt"){ p =>
  ((kToNumIndirectGenes.toList.sortWith((t1,t2)=> t1._1<t2._1)).::(("k"->"num_genes"))) map { tpl => tpl._1 + "," + tpl._2} foreach {p.println}
}

//--------------------------------------------------------------------------------
// gather data for analysis of number of concepts that directly annotate k genes
//--------------------------------------------------------------------------------
val conceptDirectCount = (Map[Int, Int]() /: ontology.values) { (m,c) =>
  ontology.countDirectFor(c) match{
    case Some(i) => m.get(i) match{
      case Some(cnt) => m + (i->(cnt+1))
      case _ => m + (i->1)
    }
    case _ => m
  }
}
printToFile("../data/num_concepts_K_direct_annotations.txt"){ p =>
  ((conceptDirectCount.toList.sortWith((t1,t2)=> t1._1<t2._1)).::(("k"->"num_concepts"))) map { tpl => tpl._1 + "," + tpl._2} foreach {p.println}
}
//--------------------------------------------------------------------------------
// gather data for analysis of number of concepts that indirectly annotate k genes
//--------------------------------------------------------------------------------
val conceptIndirectCount = (Map[Int, Int]() /: ontology.values) { (m,c) =>
  ontology.countDescendantFor(c) match{
    case Some(i) => m.get(i) match{
      case Some(cnt) => m + (i->(cnt+1))
      case _ => m + (i->1)
    }
    case _ => m
  }
}
printToFile("../data/num_concepts_K_indirect_annotations.txt"){ p =>
  ((conceptIndirectCount.toList.sortWith((t1,t2)=> t1._1<t2._1)).::(("k"->"num_concepts"))) map { tpl => tpl._1 + "," + tpl._2} foreach {p.println}
}

//--------------------------------------------------------------------------------
// gather data for analysis of information content of concpets by rank order
//--------------------------------------------------------------------------------
val orderedInformationContent = (ontology.values map { c => ontology.information_content(c)}).filter(_>=0).toList.sortWith((d1,d2)=>d1<d2)

printToFile("../data/informationContentRank.txt"){ p =>
  ((orderedInformationContent.zipWithIndex).::("rank"->"Information_Content")) map { tpl => tpl._2 + "," + tpl._1} foreach {p.println}
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

println("Done")