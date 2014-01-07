/**
 * @author masinoa
 * Dec 3, 2012
 * Copyright (c) 2012, The Children's Hospital of Philadelphia All rights reserved.
 */
package edu.chop.cbmi.phenomantics.api.ontologies
import scala.language.postfixOps
import scala.annotation.tailrec
import scala.collection.immutable.{ MapLike, HashMap }
import scala.math

/**
 * Class representing an immutable Ontology concept, that is a key, value pair related to other key/value pairs
 * in a hierarchy of parent/child relationships
 * @param key - K - key used for this concept
 * @param value - V - value for this concept
 * @param parent_keys - direct ancestors of the concept
 * @param children_keys - direct descendants of the concept
 */
case class Concept[K, V](key: K, value: V, parent_keys: Set[K] = Set.empty[K], children_keys: Set[K] = Set.empty[K]) {

  def add_parent(pkey: K) : Concept[K,V] = new Concept(key, value, parent_keys + pkey, children_keys)

  def add_parent(p: Concept[K, V]) : Concept[K,V] = add_parent(p.key)
  
  def add_child(ckey: K) : Concept[K,V] = new Concept(key, value, parent_keys, children_keys + ckey)
  
  def add_child(c: Concept[K, V]) : Concept[K,V] = add_child(c.key)

}

object Concept extends{

  /**
   * given a map from a concept to its parent Concepts
   * this method constructs a Set of Concept objects with correct parent and children keys
   */
  def apply[K,V](m : Map[Concept[K,V], Set[Concept[K,V]]]) : Set[Concept[K,V]] = {
    //first add parent keys to concepts this is just a map of the map
    val withParentKeys = m map { tpl => 
    	val c = tpl._1
    	val parents = tpl._2
    	(c /: parents) { (uc, p) => uc.add_parent(p)}
    }
    //add children keys to these
    withParentKeys map { c => 
    	(c /: withParentKeys) {(parent, child_?) => 
    		if(child_?.parent_keys.contains(parent.key)) parent.add_child(child_?)
    		else parent
    	}
    } toSet
  }
}

/**
 * Class representing an immutable Ontology
 */
class Ontology[K, V](concepts: Set[Concept[K, V]]) extends Map[K, Concept[K, V]] with MapLike[K, Concept[K, V], Ontology[K, V]] {
  //checks that concepts form a valid Ontology set
  require(Ontology.isValidOntology_?(concepts))

  private val concept_map = (HashMap[K, Concept[K, V]]() /: concepts) { (m, c) => m + (c.key -> c) }

  override def empty = Ontology.empty[K, V]

  override def get(key: K) = concept_map.get(key)

  /**
   * required method for extending map. This method will not return an Ontology if V is not of type Concept[K,V]
   */
  override def +[B1 >: Concept[K, V]](kv: (K, B1)) = {
    if (kv._2.isInstanceOf[Concept[K, V]]) new Ontology(concepts + kv.asInstanceOf[(K, Concept[K, V])]._2)
    else concept_map + kv
  }

  def iterator = concept_map.iterator

  def -(key: K) = get(key) match {
    case Some(c) => new Ontology(concepts - c)
    case _ => new Ontology(concepts)
  }

  /**
   * creates a new Ontology instance whose elements consist of the new element plus those in this Ontology
   */
  def +(v: Concept[K, V]) = new Ontology(concepts + v)

  /**
   * returns the parents of the concept with this key if it exists in this Ontology
   */
  def parents(key: K): Option[Set[Concept[K, V]]] = get(key) match {
    case Some(concept) => Some((concept.parent_keys map {k => concept_map.get(k)}).flatten)
    case _ => None
  }

  /**
   * returns the parents of the concept if it exists in this Ontology
   */
  def parents(v: Concept[K, V]): Option[Set[Concept[K, V]]] = parents(v.key)

  /**
   * returns the children of the concept with this key if it exists in this Ontology
   */
  def children(key: K): Option[Set[Concept[K, V]]] = get(key) match {
    case Some(concept) => Some((concept.children_keys map {k => concept_map.get(k)}).flatten)
    case _ => None
  }

  /**
   * returns the children of the concept if it exists in this Ontology
   */
  def children(v: Concept[K, V]): Option[Set[Concept[K, V]]] = children(v.key)

  @tailrec
  private def find_ancestors(current_ancestors: Option[Set[Concept[K, V]]], this_generation: Option[Set[Concept[K, V]]]): Set[Concept[K, V]] = {
    val ca = current_ancestors match {
      case Some(ca) => ca
      case _ => Set.empty[Concept[K, V]]
    }
    val next_generation = this_generation match {
      case Some(tg) => (Set.empty[Concept[K, V]] /: tg) { (s, c) =>
        parents(c) match {
          case Some(p) => s ++ p
          case _ => s
        }
      }
      case _ => Set.empty[Concept[K, V]]
    }
    if (next_generation.isEmpty) ca
    else find_ancestors(Some(ca ++ next_generation), Some(next_generation))
  }

  /**
   * @param key: key for Concept for which ancestors are sought
   * @return : Option Set Concept K,V - The set of all ancestors of this key, None if key is not in this Ontology
   */
  final def ancestors(key: K): Option[Set[Concept[K, V]]] = get(key) match {
    case Some(c) => Some(find_ancestors(None, Some(Set(c))))
    case _ => None
  }

  /**
   * @param c: Concept for which ancestors are sought
   * @return : Option[Set[Concept[K,V  - The set of all ancestors of this concept, None if concept is not in this Ontology
   */
  final def ancestors(c: Concept[K, V]): Option[Set[Concept[K, V]]] = ancestors(c.key)

  def commonAncestors(c1: Concept[K,V], c2: Concept[K,V]) : Set[Concept[K,V]] = ancestors(c1) match{
    case Some(s1) => ancestors(c2) match{
      case Some(s2) => s1 & s2
      case _ => Set.empty[Concept[K,V]]
    }
    case _ => Set.empty[Concept[K,V]]
  }

  @tailrec
  private def find_descendants(current_descendants: Option[Set[Concept[K, V]]], this_generation: Option[Set[Concept[K, V]]]): Set[Concept[K, V]] = {
    val ca = current_descendants match {
      case Some(ca) => ca
      case _ => Set.empty[Concept[K, V]]
    }
    val next_generation = this_generation match {
      case Some(tg) => (Set.empty[Concept[K, V]] /: tg) { (s, c) =>
        children(c) match {
          case Some(kids) => s ++ kids
          case _ => s
        }
      }
      case _ => Set.empty[Concept[K, V]]
    }
    if (next_generation.isEmpty) ca
    else find_descendants(Some(ca ++ next_generation), Some(next_generation))
  }

  /**
   * @param key: key for Concept for which descendants are sought
   * @return : Option[Set[Concept[K,V - The set of all descendants of this key, None if key is not in this Ontology
   */
  final def descendants(key: K): Option[Set[Concept[K, V]]] = get(key) match {
    case Some(c) => Some(find_descendants(None, Some(Set(c))))
    case _ => None
  }

  /**
   * @param c: Concept for which descendants are sought
   * @return : Option[Set[Concept[K,V - The set of all descendants of this concept, None if concept is not in this Ontology
   */
  final def descendants(c: Concept[K, V]): Option[Set[Concept[K, V]]] = descendants(c.key)

}

object Ontology extends {
  def empty[K, V] = new Ontology[K, V](Set.empty[Concept[K, V]])

  def apply[K,V]() = empty[K,V]

  def apply[K, V](concepts: Concept[K, V]*) = new Ontology[K, V](concepts.toSet)
  
  def apply[K,V](concepts: Set[Concept[K,V]]) : Ontology[K,V] = apply(concepts.toSeq: _*)

  //checks that the set of concepts represents a valid ontology by satisfying the following conditions
  //1. there are no cycles in relations
  //2. the set is self contained, i.e. all parents/children of a concept are themselves contained in the set
  //3. concepts should not be there own parents/child - this is subset of 1 but easier to check
  //4. concept keys should not be repeated
  /**
   * @todo Complete cycle check
   * @todo Create Informative Exceptions
   */
  private def isValidOntology_?[K, V](concepts: Set[Concept[K, V]]): Boolean = {
    val concept_keys = concepts map {c => c.key}

    //check that all parents/children of a concept are themselves concepts
    //check that concepts do not refer / contain themselves in their parent/children set
    val isValid = ((true, Set.empty[K]) /: concepts) { (tpl, c) =>
      val b = tpl._1
      val previous_keys = tpl._2
      val pbool = (true /: c.parent_keys) { (pb, parent) =>
        pb && concept_keys.contains(parent) && (c.key != parent)
      }
      val cbool = (true /: c.children_keys) { (cb, child) =>
        cb && concept_keys.contains(child) && (c.key != child)
      }
      val noRepeats = !previous_keys.contains(c.key)
      (b && pbool && cbool && noRepeats, previous_keys + c.key)
    }

    isValid._1
  }

}

class AnnotatingOntology[G, K, V](annotations: Map[G, Set[Concept[K, V]]], concepts: Set[Concept[K, V]])
  extends Ontology(concepts){
  //checks that annotations are valid
  //NOTE: check on valid Ontology is done by super
  require(AnnotatingOntology.isValidAnnotations_?(annotations, concepts))

  override def empty = AnnotatingOntology.empty[G, K, V]
  
  override def +[B1 >: Concept[K, V]](kv: (K, B1)) = {
    if (kv._2.isInstanceOf[Concept[K, V]]) new AnnotatingOntology(annotations, concepts + kv.asInstanceOf[(K, Concept[K, V])]._2)
    else super.+(kv)
  }
  
   override def -(key: K) : AnnotatingOntology[G,K,V] = get(key) match {
    case Some(c) =>{
      val newAnnotations = annotations map { tpl => 
       tpl._1 -> (tpl._2 filterNot(_.key == key))  
      }
      new AnnotatingOntology(newAnnotations, concepts - c)
    } 
    case _ => new AnnotatingOntology(annotations, concepts)
  }
   
   /**
   * creates a new AnnotatingOntology instance whose elements consist of the new element plus those in this AnnotatingOntology
   * @param c - concept to add to Ontology
   * @param items - objects annotated by c
   */
  def +(c: Concept[K, V], items: Set[G]) = {
    val newAnnotations = (annotations /: items) { (m, item) =>
      m.get(item) match {
        case Some(s) => m + (item -> (s+c))
        case _ => m + (item -> Set(c))
      }
    }
    new AnnotatingOntology[G,K,V](newAnnotations, concepts + c)
  }

  override def +(c: Concept[K,V]) = this.+(c, Set.empty[G])

  /**
   * creates a new AnnotatingOntology instance whose elements consist of the new element plus those in this AnnotatingOntology
   * @param tpl - concept to add to Ontology,  objects annotated by c
   * @return
   */
  def +(tpl: (Concept[K,V], Set[G])) : AnnotatingOntology[G, K, V] = this.+(tpl._1, tpl._2)

  lazy val itemsAnnotatedAll = annotations.keys

  //map from concept to terms it directly annotates
  lazy private val inverted_annotations = {
     val initial: Map[Concept[K, V], Set[G]] = concepts map { c => c->Set.empty[G]} toMap

    (initial /: annotations){ (m, tpl) =>
     val item = tpl._1
     val item_concepts = tpl._2
     (m /: item_concepts) { (nm, concept) =>
       nm.get(concept) match {
         case Some(s) => nm + (concept -> (s + item))
         case _ => nm + (concept -> Set(item))
       }
     }
    }
  }
  
  //map from concept to terms it or any of its descendants annotate
  lazy private val inverted_descendant_annotations = (Map[Concept[K,V], Set[G]]() /: inverted_annotations){ (m, tpl) =>
  	val concept = tpl._1
  	val items = tpl._2  //items annotated directly by concept
    //if c annotates a concept, it's ancestors will also be considered to annotate the concept, which can be restated
    //as saying that a concept annotates an item if it or any of its descendants annotate the item
  	val withAncestors = ancestors(concept) match{
  	  case Some(s) => s + concept
  	  case _ =>  Set(concept)
  	}
  	(m /: withAncestors) {(nm, c) => 
  		nm.get(c) match{
  		  case Some(s) => nm + (c -> (s ++ items))
  		  case _ => nm + (c -> items)
  		}
  	}
  }
  
  //map from concept to total annotation count, including descendants
  lazy private val descendant_count_map: Map[Concept[K, V], Int] = concepts.map{concept =>
    inverted_descendant_annotations get(concept) match{
      case Some(s) => concept->((0 /: s) {(cnt, g) => cnt + 1})
      case _ => concept->0
    }
  }.toMap

  //map from concept to number of direct annotation counts
  lazy private val direct_count_map = {
    val initial : Map[Concept[K,V], Int] = (this.concepts map { c => c->0} toMap)
    (initial /: annotations.values) { (m, cons) =>
        (m /: cons) { (nm, concept) =>
            nm.get(concept) match {
              case Some(count) => nm + (concept -> (count + 1))
              case _ => nm + (concept -> 1)
            }
        }
    }
  }
  
  //total annotations including ancestral
  lazy private val totalAnnotationCount: Int = annotations.size //number of items annotated



  //concepts that don't annotate anything either directly OR through descendants
  lazy private val nonAnnotatingConceptsSet  = inverted_descendant_annotations.filter{(tpl) => tpl._2.isEmpty}.keySet

  //concepts that do annotate something either directly OR through descendants
  lazy private val annotatingConceptsSet = inverted_descendant_annotations.filterNot{(tpl)=> tpl._2.isEmpty}.keySet

  /**
   * all concepts that do not annotate any items directly OR through their descendents
   * @return Set[Concpet[K,V]]
   */
  def nonAnnotatingConcepts : Set[Concept[K,V]] = nonAnnotatingConceptsSet

  def annotatingConcepts : Set[Concept[K,V]] = annotatingConceptsSet

  /**
   * @return - the total number of annotations where a concept is considered to annotate an item if it or ANY of
   * its descendants directly annotates the item
   */
  def countAll = totalAnnotationCount

  def annotatingConcpetsSortedKey = annotatingConcepts.toList.sortBy(_.key.toString)

  /**
   * @param c - Concept
   * @return - Option[Int] - the number of items directly annotated by c
   */
  def countDirectFor(c:Concept[K,V]) = direct_count_map.get(c)

  /**
   * @param k - key for Concept
   * @return Option[Int] - the number of items directly annotated by Concept with key k
   */
  def countDirectFor(k: K) : Option[Int] = get(k) match{
    case Some(c) => countDirectFor(c)
    case _ => None
  }
  
  /**
   * @param c- Concept
   * @return - Option[Int] - the number of items directly annotated by c or ANY of its descendants
   */
  def countDescendantFor(c: Concept[K,V]) = descendant_count_map.get(c)

  /**
   * @param k - key for Concept c
   * @return - Option[Int] - the number of items directly annotated by Concept with key k or ANY of its descendants
   */
  def countDescendantFor(k: K) : Option[Int] = get(k) match{
    case Some(c) => countDescendantFor(c)
    case _ => None
  }
  
  /**
   * @param c - Concept
   * @return Option[Set[G]] - items directly annotated by c
   */
  def itemsAnnotatedDirectlyBy(c: Concept[K,V]) = inverted_annotations.get(c)

  /**
   *
   * @param k - key for Concept
   * @return - Option[Set[G]] - items directly annotated by c
   */
  def itemsAnnotatedDirectlyBy(k: K) : Option[Set[G]] = get(k) match{
    case Some(c) => itemsAnnotatedDirectlyBy(c)
    case _ => None
  }
  
  /**
   * @param c - Concept
   * @return Option[Set[G]] - items directly annotated by c or ANY of its descendants 
   */
  def itemsAnnotatedDescendantBy(c: Concept[K,V]) = inverted_descendant_annotations.get(c)

  def itemsAnnotatedDescendantBy(k: K) : Option[Set[G]] = get(k) match{
    case Some(c) => itemsAnnotatedDescendantBy(c)
    case _ => None
  }

  def annotationsFor(item: G) = annotations.get(item)

  /**
   * finds the common ancestor of c1 and c2 that is used as an annotation (either directly or
   * through its descendants) the minimum number of times compared to all other common ancestors. Common
   * ancestors that do not annotate are ignored - the existence of such a common ancestor implies that
   * c1 or c2 (or both) do not annotate.
   * @param c1
   * @param c2
   * @return  (Concept,Int) whose first element is the common ancestor and second is the number of times it
   *          is used to annotate items
   */
  def minAnnotatedCommonAncestor(c1: Concept[K,V], c2: Concept[K,V]) : (Concept[K,V], Int) = {
    def checkAncestors() : (Concept[K,V],Int)= {
      val c1Ancestors = ancestors(c1).getOrElse(Set.empty[Concept[K,V]])
      val c2Ancestors = ancestors(c2).getOrElse(Set.empty[Concept[K,V]])

      val caProper = commonAncestors(c1,c2)

      val ca = if(c1Ancestors.contains(c2)) caProper + c2
               else if(c2Ancestors.contains(c1)) caProper + c1
               else caProper

      if(ca.isEmpty){ //one of the concepts is the root concept
        val root = parents(c1) match{
          case Some(s) => if(s.isEmpty)c1 else c2
          case _ => c1
        }
        (root,countDescendantFor(root).getOrElse(Int.MaxValue))
      }else{
        val h = ca.head
        ((h,countDescendantFor(h).getOrElse(Int.MaxValue)) /: ca.tail) { (t,c) =>
          val cn = countDescendantFor(c).getOrElse(Int.MaxValue)
          if(cn<t._2) (c,cn) else t
        }
      }
    }
    if(c1.key==c2.key){
      countDescendantFor(c1) match{
        case Some(i)=> if(i>0) (c1,i) else checkAncestors()
        case _ => checkAncestors()
      }
    }else checkAncestors()
  }

  /**
   *
   * @param c : Concept[K,V]
   * @return : frequency of c, the proportion of annotated items, G, that are annotated by c or ANY of its descendants
   */
  def frequency(c: Concept[K,V]) : Double = frequency(c.key)

  def frequency(k: K) = countDescendantFor(k) match{
    case Some(count) => count/countAll.toDouble
    case _ => -1.0
  }

  def information_content(k : K) : Double = {
    val f = frequency(k)
    if(f>0) -math.log(f) else 0.0
  }

  /**
   * @param c : Concept[K,V]
   * @return : the information content for this concept, -1.0 if the concept (or at least one of its descendants) does
   *         not annotate an object
   */
  def information_content(c: Concept[K,V]): Double = information_content(c.key)

}

object AnnotatingOntology extends {
  def empty[G, K, V] = new AnnotatingOntology[G,K,V](Map[G, Set[Concept[K,V]]](), Set.empty[Concept[K,V]])

  def apply[G, K, V]() = empty[G, K, V]

  def apply[G, K, V](annotations: Map[G, Set[Concept[K,V]]], concepts: Set[Concept[K,V]]) = new AnnotatingOntology(annotations, concepts)

  def apply[G, K, V](concepts: Set[Concept[K,V]]) : AnnotatingOntology[G,K,V] = apply(Map[G, Set[Concept[K,V]]](), concepts)

  def apply[G, K, V](annotations: Map[G, Set[Concept[K,V]]], concepts: Concept[K,V]*) : AnnotatingOntology[G,K,V] = apply(annotations, concepts.toSet)

  def apply[G, K, V](concepts: Concept[K,V]*) : AnnotatingOntology[G,K,V] = apply(Map[G, Set[Concept[K,V]]](), concepts.toSet)

  //checks that the annotations are valid
  //1. concept used to annotate an item should be in the concepts set
  /**
   *
   * @todo create more informative exception
   */
  private def isValidAnnotations_?[G,K,V](annotations: Map[G, Set[Concept[K, V]]], concepts: Set[Concept[K,V]]) : Boolean = {
    (true /: annotations.values) { (b, s) =>
      (b /: s) { (nb, c) => nb && concepts.contains(c)}
    }
  }
}

/**
 * instantiates similarity methods using information content, only mixed in with AnnotatingOntology
 */

trait Similarity[G, K, V]{

  this: AnnotatingOntology[G, K, V] =>

  def asymmetricMaxSimilarity(s1:Set[Concept[K,V]], s2:Set[Concept[K,V]])(simX: (Concept[K,V],Concept[K,V])=>Double) : Double = {
    (0.0 /: s1){ (csum, c1) =>
      csum + (0.0 /: s2){ (cmax, c2) => math.max(cmax, simX(c1,c2))}
    } / s1.size.toDouble
  }

  def symmetricMaxSimilarity(s1:Set[Concept[K,V]], s2:Set[Concept[K,V]])(simX: (Concept[K,V],Concept[K,V])=>Double) : Double = {
    (asymmetricMaxSimilarity(s1,s2)(simX) + asymmetricMaxSimilarity(s2,s1)(simX)) * 0.5
  }

  def sim1(c1 : Concept[K,V], c2: Concept[K,V]) = {
    val (c,nMica) = minAnnotatedCommonAncestor(c1,c2)
    math.log(countAll/nMica.toDouble)
  }

  def asymMaxSim1(s1: Set[Concept[K,V]], s2: Set[Concept[K,V]]) = {
    asymmetricMaxSimilarity(s1,s2)(sim1)
  }

  def symMaxSim1(s1: Set[Concept[K,V]], s2: Set[Concept[K,V]]) = {
    symmetricMaxSimilarity(s1,s2)(sim1)
  }

  def sim2(c1 : Concept[K,V], c2: Concept[K,V]) = {
    if(c1==c2)1.0
    else{
      val np1 = countDescendantFor(c1).getOrElse(0)
      val np2 = countDescendantFor(c2).getOrElse(0)
      val (c,nMica) = minAnnotatedCommonAncestor(c1,c2)
      np1 * np2 /(nMica*nMica).toDouble
    }
  }

  def symMaxSim2(s1: Set[Concept[K,V]], s2: Set[Concept[K,V]]) = {
    symmetricMaxSimilarity(s1,s2)(sim2)
  }

  def asymMaxSim2(s1: Set[Concept[K,V]], s2: Set[Concept[K,V]]) = {
    asymmetricMaxSimilarity(s1,s2)(sim2)
  }

}

