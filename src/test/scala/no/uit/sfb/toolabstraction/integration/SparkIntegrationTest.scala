package no.uit.sfb.toolabstraction.integration

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

class SparkIntegrationTest extends FunSpec with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }

  describe("is") {
    pending
  }

}
