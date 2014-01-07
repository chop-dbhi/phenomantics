/**
 * masinoa
 * Dec 4, 2012
 * Copyright (c) 2012, The Children's Hospital of Philadelphia All rights reserved.
 */
package edu.chop.cbmi.phenomantics.api.ontologies
import org.scalatest.FunSpec
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfter
import scala.language.reflectiveCalls

class OntologySpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter {

  def fixture() = {
    new {
      val a1 = Concept("a1",1)
      val b1 = Concept("b1",2)
      val b2 = Concept("b2",3)
      val b3 = Concept("b3",4)
      val c1 = Concept("c1",5)
      val c2 = Concept("c2",6)
      val c3 = Concept("c3",7)
      val d1 = Concept("d1",8)
      val d2 = Concept("d2",9)
      val d3 = Concept("d3",10)
      val parents = Map(a1-> Set.empty[Concept[String,Int]],
        b1 -> Set(a1),
        b2 -> Set(a1),
        b3 -> Set(a1),
        c1 -> Set(b1),
        c2 -> Set(b2),
        c3 -> Set(b2, b3),
        d1 -> Set(c2),
        d2 -> Set(c3),
        d3 -> Set(b1, c2))
    }
    
  }

  val f = fixture()

  describe("A Concept"){
    it("should be an immutable container for items related by parent/child relationships"){
      Given("a concept")
      val a1 = f.a1
      val b1 = f.b1
      When("adding a parent")
      val b11 = b1.add_parent(a1)
      Then("the original concept should be unchanged")
      b1.parent_keys should equal(Set.empty[String])
      b1 should not equal(b11)
      b1.parent_keys should not equal(b11.parent_keys)
    }
  }
  
  describe("The Concept Companion Object"){
    it("should create a set of concepts with valid parent/child relations"){
      Given("a map from a concept to it's parent concepts where each concept has empty parent, child sets")
      val concepts = Concept(f.parents)
      
      Then("the returned set should be the same size as the map")
      concepts.size should equal(f.parents.size)

      And("each concept should have a reference to it's parent keys")
      val keyed_parents = f.parents map (tpl => tpl._1.key -> tpl._2)
      concepts foreach {concept => 
      	keyed_parents.get(concept.key) match{
      	  case Some(parents) => (true /: parents){(b,p) => b && concept.parent_keys.contains(p.key)} should equal(true)
      	  case _ => fail
      	}
      }

      And("each concept should have a reference to it's children's keys")
      (true /: keyed_parents) { (b, tpl) =>
        val child_key = tpl._1
        val parent_concepts = tpl._2
        (b /: parent_concepts) { (b, pc) =>
          b && (concepts.find(_.key == pc.key) match {
            case Some(c) => c.children_keys.contains(child_key)
            case _ => false
          })
        }
      } should equal(true)
      
    }//end it
  }//end describe Concept Companion Object
  
  describe("An Ontology") {
    it("should be an immutable collection Concepts with relation functions") {
      Given("an empty set of concepts")
      val emptyOntology: Ontology[String,Int] = Ontology[String,Int]()

      Then("the Ontology Companion Object should provide the empty Ontology")
      emptyOntology.iterator.length should equal(0)

      Then("adding a concept to the ontology should produce a new Ontology")
      val x = Concept("x",1)
      val eo2 = emptyOntology + x
      eo2 should not equal(emptyOntology)
      eo2.get("x") should equal(Some(x))
      eo2.children(x) should equal (Some(Set.empty[Concept[String,Int]]))

      And("the original ontology should be unchanged")
      emptyOntology.get("x") should equal(None)

      Given("an invalid Concept set that has parent/children references to Concepts not in the set")
      val y = Concept("x", 1, Set("y"))
      Then("creating an Ontology should throw an IllegalArgumentException failed requirements")
      evaluating{ Ontology(y) } should produce[IllegalArgumentException]

      Given("an invalid Concept set that has repeated keys")
      Then("creating an Ontology should throw an IllegalArgumentException failed requirements")
      evaluating{ Ontology(Concept("x",1), Concept("x",2)) } should produce[IllegalArgumentException]

      Given("an invalid Concept set that has repeated keys")
      Then("creating an Ontology should throw an IllegalArgumentException failed requirements")
      evaluating{ Ontology(Concept("x",1, Set("x")), Concept("y",2)) } should produce[IllegalArgumentException]
      evaluating{ Ontology(Concept("x",1, Set.empty[String], Set("x")), Concept("y",2)) } should produce[IllegalArgumentException]


      Given("a non-empty set of concepts")
      Then("the Ontology Companion Object should provide a populated Ontology")
      val concepts = Concept(f.parents)
      val o = Ontology(concepts)

      Then("it should have an iterator of length equal to the concept set length")
      o.iterator.length should equal(concepts.size)

      And("it should be possible to obtain the parents for each concept from the ontology or None if the concept is not in the ontology")
      concepts foreach{ concept =>
      	o.parents(concept) match {
      	  case Some(parents) => ((parents map {_.key})==concept.parent_keys) should equal(true)
      	  case _ => fail
      	}
      }
      o.parents(Concept("x",1)) should equal(None)

      And("all the descendants or None if the concept is not in the ontology")
      o.ancestors(Concept("x",1)) should equal(None)
      o.ancestors(f.a1.key) should equal(Some(Set.empty[Concept[String,Int]]))

      def akt(c:Concept[String,Int], fs: Set[Concept[String,Int]]) : Boolean = {
        val keys = fs map {_.key}
        o.ancestors(c.key) match{
          case Some(s) => (s map {_.key}) == keys
          case _ => false
        }
      }

      val r = Set(f.a1)
      akt(f.b1, r) should equal(true)
      akt(f.b2, r) should equal(true)
      akt(f.b3, r) should equal(true)
      akt(f.c1, Set(f.a1, f.b1)) should equal(true)
      akt(f.c2, Set(f.b2, f.a1)) should equal(true)
      akt(f.c3, Set(f.b2, f.b3, f.a1)) should equal(true)
      akt(f.d1, Set(f.c2, f.b2, f.a1)) should equal(true)
      akt(f.d2, Set(f.c3, f.b2, f.b3, f.a1)) should equal(true)
      akt(f.d3, Set(f.b1, f.a1, f.c2, f.b2)) should equal(true)

      And("it should be possible to obtain the children for each concept from the onotology or None if the concept is not in the ontology")
      o.children(Concept("x",1)) should equal(None)
      ((true /: concepts){ (b, concept) =>
      	o.children(concept) match{
      	  case Some(s) => b && ((s map {_.key}) == concept.children_keys)
      	  case _ => false
      	}
      }) should equal(true)

      And("it should be possible to obtain all the descendants, or None if the concept is not in the ontology")
      o.descendants(Concept("x",1)) should equal(None)
      ((true /: List(f.d3, f.d2, f.d1, f.c1)) {(b,c) =>
        b && o.descendants(c.key)==Some(Set.empty[Concept[String,Int]])
      }) should equal(true)

      def dkt(c:Concept[String,Int], fs: Set[Concept[String,Int]]) : Boolean = {
        val keys = fs map {_.key}
        o.descendants(c.key) match{
          case Some(s) => (s map {_.key}) == keys
          case _ => false
        }
      }
      dkt(f.a1, Set(f.b1, f.b2, f.b3, f.c1, f.c2, f.c3, f.d1, f.d2, f.d3)) should equal(true)
      dkt(f.b1, Set(f.c1, f.d3)) should equal(true)
      dkt(f.b2, Set(f.c2, f.c3, f.d1, f.d2, f.d3)) should equal(true)
      dkt(f.b3, Set(f.c3, f.d2)) should equal(true)
      dkt(f.c2, Set(f.d1, f.d3)) should equal(true)
      dkt(f.c3, Set(f.d2)) should equal(true)
    }
  }

  describe("An AnnotatingOntology"){
    it("should be Ontology with some of the Concepts annotated to items"){
      Given("an empty set of concepts and annotations")
      type G = Double
      type K = String
      type V = Int
      val eao: AnnotatingOntology[G,K,V] = AnnotatingOntology[G,K,V]()

      Then("the AnnotatingOntology Companion Object should provide the empty AnnotatingOntology")
      eao.iterator.length should equal(0)

      Then("adding a concept to the AnnotatingOntology should produce a new AnnotatingOntology")
      val x = Concept("x",1)
      val eo2: AnnotatingOntology[G, K, V] = eao + x
      eo2 should not equal(eao)
      eo2.get("x") should equal(Some(x))
      eo2.children(x) should equal (Some(Set.empty[Concept[String,Int]]))
      val y = Concept("y",2)
      (eo2 + (y->Set(1.0))).get("y") should equal(Some(y))
      (eo2 + y).get("y") should equal(Some(y))

      And("the original ontology should be unchanged")
      eao.get("x") should equal(None)

      /*And("Concept set validity should be checked by the parent Ontology class, so given an invalid concept set")
      val z = Concept("x", 1, Set("z"))
      Then("creating an AnnotatingOntology should throw an IllegalArgumentException failed requirements")
      evaluating{ AnnotatingOntology(z) } should produce[IllegalArgumentException]  */

      Given("a non-empty set of concepts and annotations")
      Then("the AnnotatingOntology Companion Object should provide a populated AnnotatingOntology")
      val concepts = Concept(f.parents)
      def g(k:String) = concepts.find(_.key == k) match {
        case Some(c) => c
        case _ => Concept("XXX", -1)
      }
      val annotations = Map(1.0->Set(g(f.c1.key), g(f.d1.key)),
                            2.0->Set(g(f.b2.key)),
                            3.0->Set(g(f.b3.key), g(f.d3.key),g(f.c1.key)))
      val ao = AnnotatingOntology(annotations, concepts)

      And("it should provide direct annotation counts")
      ao.countDirectFor(f.c1.key) should equal(Some(2))
      (true /: List(f.d1, f.b2, f.b2, f.b3, f.d3)) { (b,c) => b && ao.countDirectFor(c.key)==Some(1)} should equal(true)
      (true /: List(f.a1, f.b1, f.c2, f.c3, f.d2)) { (b,c) => b && ao.countDirectFor(c.key)==Some(0)} should equal(true)

      And("it should provide descendant annotation counts")
      ao.countDescendantFor(f.b1.key) should equal(Some(2))
      ao.countDescendantFor(f.b2.key) should equal(Some(3))
      ao.countDescendantFor(f.b3.key) should equal(Some(1))
      ao.countDescendantFor(f.c1.key) should equal(Some(2))
      ao.countDescendantFor(f.c2.key) should equal(Some(2))
      ao.countDescendantFor(f.c3.key) should equal(Some(0))
      ao.countDescendantFor(f.d1.key) should equal(Some(1))
      ao.countDescendantFor(f.d2.key) should equal(Some(0))
      ao.countDescendantFor(f.d3.key) should equal(Some(1))
      ao.countDescendantFor("x") should equal(None)

      And("it should provide items directly annotated by concpets")
      def dat(c:Concept[String,Int], d: Double*) : Boolean = {
        ao.itemsAnnotatedDirectlyBy(c.key)==Some(Set(d: _*))
      }
      dat(f.c1, 1.0, 3.0) should equal(true)
      dat(f.d1, 1.0) should  equal(true)
      dat(f.b2, 2.0) should equal(true)
      dat(f.b3, 3.0) should equal(true)
      dat(f.d3, 3.0) should equal(true)
      (true /: List(f.a1, f.b1, f.c2, f.c3, f.d2)){(b,c) => ao.itemsAnnotatedDirectlyBy(c.key)==Some(Set.empty[Double])} should equal(true)
      ao.itemsAnnotatedDirectlyBy("x") should equal(None)

      And("it should provide items annotated by descendants")
      def aat(c:Concept[String, Int], d: Double*) : Boolean = {
        ao.itemsAnnotatedDescendantBy(c.key)==Some(Set(d: _*))
      }
      aat(f.a1, 1.0, 2.0, 3.0) should equal(true)
      aat(f.b1, 1.0, 3.0) should equal(true)
      aat(f.b2, 2.0, 1.0, 3.0) should equal(true)
      aat(f.b3, 3.0) should equal(true)
      aat(f.c1, 1.0, 3.0) should equal(true)
      aat(f.c2, 1.0, 3.0) should equal(true)
      ao.itemsAnnotatedDescendantBy(f.c3.key) should equal(Some(Set.empty[Double]))
      aat(f.d1, 1.0) should equal(true)
      ao.itemsAnnotatedDescendantBy(f.d2.key) should equal(Some(Set.empty[Double]))
      aat(f.d3, 3.0) should equal(true)
      ao.itemsAnnotatedDescendantBy("x") should equal(None)

      And("it should provide the total number of items annotated")
      ao.countAll should equal(annotations.size)

      And("the root term should, via descendants, should annotate all items")
      ao.countDescendantFor(f.a1.key) should equal(Some(annotations.size))
    }
  }

}