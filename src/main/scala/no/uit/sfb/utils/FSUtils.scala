package no.uit.sfb.utils

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Paths}

object FSUtils extends Serializable {

  def createDirectory(dirName: String): Boolean = {
    try {
      val dir = new File(dirName)
      if (dir.exists) {
        return false
      }

      dir.mkdirs()
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  def createFile(fileName: String): Boolean = {
    try {
      val f = new File(fileName)
      if(f.exists) {
        return false
      }

      val fos = new FileOutputStream(f)
      fos.close()

      true
    } catch {
      case e: Exception => {
        println(e)
        false
      }
    }
  }

  def pathsExist(paths: String*): Boolean = {
    paths.foreach{ path =>
      if(!Files.exists(Paths.get(path)))
        return false
    }

    true
  }
}
