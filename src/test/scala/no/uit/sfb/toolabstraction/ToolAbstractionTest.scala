package no.uit.sfb.toolabstraction


import java.io.InputStream

import no.uit.sfb.testdata.TestData
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}

class ToolAbstractionTest extends FunSpec with Matchers with BeforeAndAfterEach {
  val path = System.getProperty("java.io.tmpdir")

  override def beforeEach(): Unit = {
  }

}
