package no.uit.sfb.utils

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}


class FSUtilsTest extends FunSpec with Matchers with BeforeAndAfterAll {
  val cwd = new File(".").getAbsolutePath.dropRight(1)
  describe("createDirectory") {
    it("should return true upon succeeding to create directory") {
      val success = FSUtils.createDirectory(cwd + "dirname")
      val dir = new File(cwd + "dirname")

      dir.exists shouldBe true
      FileUtils.deleteDirectory(dir)
      success shouldBe true
    }
    it("should return false upon failing to create directory") {
      val existing = FSUtils.createDirectory(cwd + "dirname")
      val success = FSUtils.createDirectory(cwd + "dirname")
      val dir = new File(cwd + "dirname")

      success shouldBe false
      FileUtils.deleteDirectory(dir)
      dir.exists shouldBe false
    }
  }
  describe("createFile") {
    it("should return true upon succeeding to create file") {
      val success = FSUtils.createFile(cwd + "filename")
      val f = new File(cwd + "filename")

      f.exists shouldBe true
      f.delete()
      success shouldBe true
    }
    it("should return false upon failing to create file"){
      val existing = FSUtils.createFile(cwd + "filename")
      val success = FSUtils.createFile(cwd + "filename")
      val f = new File(cwd + "filename")

      success shouldBe false
      f.delete()
      f.exists shouldBe false
    }
  }

}
