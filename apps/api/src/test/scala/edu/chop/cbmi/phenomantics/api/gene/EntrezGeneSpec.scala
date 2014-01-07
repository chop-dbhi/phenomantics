/**
 * masinoa
 * Jan 16, 2013
 * Copyright (c) 2012, The Children's Hospital of Philadelphia All rights reserved.
 */
package edu.chop.cbmi.phenomantics.api.gene

import org.scalatest.FunSpec
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfter
import java.io.InputStream
import scala.language.reflectiveCalls

/**
 * @author masinoa
 *
 */
class EntrezGeneSpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter {

  val EXPECTED_ENTREZ_GENE_COUNT = 2488

  describe("The EntrezGeneSet Object") {
    it("provides access to specialized Entrez Gene sets") {
      EntrezGeneSet.hpo_annotated_genes.find(_.id == 4018) match {
        case Some(gene) => gene.symbol should equal("LPA")
        case _ => fail
      }
      EntrezGeneSet.hpo_annotated_genes.size should equal(EXPECTED_ENTREZ_GENE_COUNT)
    }
  }
}