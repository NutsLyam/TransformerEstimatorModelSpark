import org.apache.spark.ml.param.{Param, Params}
import org.json4s.{Extraction, FullTypeHints}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization

trait MyParams extends Params {
  val targetCol = new Param[String](this, "targetCol", "Target column")

  def getTargetCol(): Option[String] = get(targetCol)

  def setTargetCol(value: String): this.type = set(targetCol, value)

  val estimatedCol = new Param[String](this, "estimatedCol", "Estimated column")

  def getEstimatedCol(): Option[String] = get(estimatedCol)

  def setEstimatedCol(value: String): this.type = set(estimatedCol, value)

  val metadataCol = new Param[MetadataClass](this, "metadataCol", "Metadata of column")

  def getMetadataCol(): Option[MetadataClass] = get(metadataCol)

  def setMetadataCol(value: MetadataClass): this.type = set(metadataCol, value)

  def jsonEncode(metadata: MetadataClass): String = {
    // implicit val formats = Serialization.formats(NoTypeHints)
    implicit val formats = {
      Serialization.formats(FullTypeHints(List(classOf[MetadataClass])))
    }
    compact(render(Extraction.decompose(metadata)))
  }

  def jsonDecode(strJson: String): MetadataClass = {
    implicit val formats = {
      Serialization.formats(FullTypeHints(List(classOf[MetadataClass])))
    }
    parse(strJson).extract[MetadataClass]

  }
}