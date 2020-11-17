package io.opentargets.etl.backend.extractors

object JsonFile {
  def unapply(path: String): Boolean = path.endsWith("json") || path.endsWith("jsonl")
}
