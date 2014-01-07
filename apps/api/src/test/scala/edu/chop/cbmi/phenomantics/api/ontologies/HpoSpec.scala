package edu.chop.cbmi.phenomantics.api.ontologies

import org.scalatest.FunSpec
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfter
import scala.collection.mutable.ArrayBuffer

class HpoSpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter {

  describe("The HPO Term Object"){
    it("should provide a Set of HPO terms with the correct alt ids"){
       HPO_Term.getTermById("HP:0200021") match{
         case Some(term) => {
           term.id should equal("HP:0200021")
           term.term should equal("Down-sloping shoulders")
           term.alt_id should equal(Set("HP:0006663", "HP:0001556"))
         }
         case _ => fail
       }

      And("a set of Concept[String,HPO_Term with the correct parent child configuration")
      HPO_Term.getConceptById("HP:0009435") match{
        case Some(concept) => {
          concept.parent_keys should equal(Set("HP:0009849","HP:0009445", "HP:0004172"))
        }
        case _ => fail
      }

      And("the set of all HPO concepts should have the correct length")
      val concepts = HPO_Term.allConcepts
      concepts.size should equal(9965)
    }
  }

  describe("The HPOEntrez Annotating Ontology with Information Similarity"){
    it("should be an Annotating Ontology with Information Similarity created by the HPOEntrezFactory"){
      val ontology = HPOEntrezFactory.informationAO
      ontology.entrezGeneById(4018) match{
        case Some(g) => ontology.annotationsFor(g) match{
          case Some(concepts) => concepts map {c => c.key} should equal(Set("HP:0001939","HP:0000006","HP:0002621"))
          case _ => fail
        }
        case _ => fail
      }

      val num_annotated_genes = 2488 //this is known from ENTREZ.txt file line count
      ontology.countAll should equal(num_annotated_genes)
      ontology.information_content("HP:0000001") should equal(0.0)
      ontology.countDescendantFor("HP:0000001") should equal(Some(num_annotated_genes))

       (ontology.values map {
          c => ontology.itemsAnnotatedDirectlyBy(c)
        }).flatten.flatten.toSet.size should equal(num_annotated_genes)

      val g = ontology.entrezGeneById(4018).get
      val gAnnotations = ontology.annotationsFor(g).get
      val phenotypes = Set(ontology.hpoConceptById("HP:0002621").get, ontology.hpoConceptById("HP:0000001").get)
      val similarity: Double = ontology.asymMaxSim1(phenotypes, gAnnotations)
      similarity should be >=(0.0)

      val ic1 = ontology.information_content(HPO_Term.getConceptById("HP:0000007").get)
      val ic2 = ontology.information_content(HPO_Term.getConceptById("HP:0000005").get)
      val d = ic1 - ic2
      val s = math.exp(-d)

      val d2 = ontology.symDistanceSim2(Set(HPO_Term.getConceptById("HP:0000007").get), Set(HPO_Term.getConceptById("HP:0000005").get))
      val s2 = ontology.sim2(HPO_Term.getConceptById("HP:0000007").get, HPO_Term.getConceptById("HP:0000005").get)

      val d3 = ontology.symDistanceSim2(Set(HPO_Term.getConceptById("HP:0000007").get), Set(HPO_Term.getConceptById("HP:0000007").get))
      val s3 = ontology.sim2(HPO_Term.getConceptById("HP:0000007").get, HPO_Term.getConceptById("HP:0000007").get)

      d3 should equal(0)
      s3 should equal(1.0)

      And("it can compute p-values from the database")
      val gene = ontology.entrezGeneById(154288).get
      val qryTerms = ontology.annotationsFor(gene).get.map{c=>c.value}.toList.sortWith{(t1,t2)=>
        val i1 = ontology.information_content(HPO_Term.getConceptById(t1.id).get)
        val i2 = ontology.information_content(HPO_Term.getConceptById(t2.id).get)
        i1<i2
      }
      val sf = ontology.SymMaxSim2
      qryTerms.foreach{term=>
        val (pval, simscore) = ontology.pvalue(gene, Set(term), sf)
        pval should be >=(0.0)
        simscore should be >=(0.0)
      }
      val (pval, simscore) = ontology.pvalue(gene, Set(HPO_Term.getTermById("HP:0002621").get), sf)
      pval should be >=(0.0)

      And("it can compute p-values from an iterable")
      val k = 1
      val result = ontology.cdfResultFor(gene, sf, k)
      val buff = new ArrayBuffer[(Double,Double)]()
      while(result.next){
        buff.+=((result.getDouble(1), result.getDouble(2)))
      }
      ontology.pvalue(simscore, buff) should equal(pval)




      //TODO build example that shows minimization of the average distance between
      //the germs in g2 - the min should be for H7

    }
  }


}