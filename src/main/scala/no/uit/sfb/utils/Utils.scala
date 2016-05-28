package no.uit.sfb.utils

import net.liftweb.json._

object Utils {
  /**
   * Wrapper for Lift-JSON to serialize and deserialize
   */
  object Json {
    implicit val formats = Serialization.formats(NoTypeHints)

    def serialize[A <: AnyRef](obj: A) = Serialization.write(obj)

    def deserialize[A <: AnyRef : Manifest](input: String): A = {
      val json = parse(input)
      json.extract[A]
    }
  }
}
