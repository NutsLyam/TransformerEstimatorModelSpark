import Main.getSparkSession
import org.json4s.{Extraction, FullTypeHints}
import org.json4s.jackson.Serialization
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.Estimator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql
import org.apache.spark.sql.types.{Metadata, StructType}


case class MetadataClass(val max: Double = 0,
                         val min: Double = 0,
                         val mean: Double = 0,
                         val std: Double = 0,
                         val q10: Double = 0,
                         val median: Double = 0,
                         val q90: Double = 0,
                         val q99: Double = 0,
                         val pearson_cor: Double = 0,
                         val spearman_cor: Double = 0,
                         val dist_value: Double = 0) {

  def putMetadata(): Metadata = {
    val metadata = new sql.types.MetadataBuilder()
      .putDouble("max", max)
      .putDouble("min", min)
      .putDouble("mean", mean)
      .putDouble("std", std)
      .putDouble("quantile_10", q10)
      .putDouble("median", median)
      .putDouble("quantile_90", q90)
      .putDouble("quantile_99", q99)
      .putDouble("cor_pearson", pearson_cor)
      .putDouble("cor_spearman", spearman_cor)
      .putDouble("distinct", dist_value)
      .build()
    metadata
  }

  def getClassMetadata(m: Metadata): MetadataClass = {
    MetadataClass(
      max = m.getDouble("max"),
      min = m.getDouble("min"),
      mean = m.getDouble("mean"),
      std = m.getDouble("std"),
      q10 = m.getDouble("quantile_10"),
      median = m.getDouble("median"),
      q90 = m.getDouble("quantile_90"),
      q99 = m.getDouble("quantile_99"),
      pearson_cor = m.getDouble("cor_pearson"),
      spearman_cor = m.getDouble("cor_spearman"),
      dist_value = m.getDouble("distinct")
    )
  }
}


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


class MyEstimator(override val uid: String) extends Estimator[MyTransformer]
  with MyParams {

  override def fit(dataset: Dataset[_]): MyTransformer = {
    val MyColumn = dataset.toDF()
    val target = $(targetCol)
    val col = $(estimatedCol)
    val spark: SparkSession = getSparkSession

    /**
      * //MyColumn.createOrReplaceTempView(col)
      * val sqlDF = spark.sql(s"SELECT MAX(${col}), MIN(${col}), AVG(${col}) as mean, STD(${col}) as std, " +
      * s"APPROX_COUNT_DISTINCT(${col}), " +
      * s"percentile_approx( ${col}, 0.1) as perc10, percentile_approx( ${col}, 0.5) as perc50," +
      * s" percentile_approx( ${col}, 0.9) as pers90, percentile_approx( ${col}, 0.99) as perc99, " +
      * s"corr(${col},${target} ) as pearson  FROM ${col}")
      **/

    val queue = MyColumn.selectExpr(s"MAX(${col})", s"MIN(${col})", s"AVG(${col}) as mean", s"STD(${col}) as std ",
      s"APPROX_COUNT_DISTINCT(${col}) ",
      s"percentile_approx( ${col}, 0.1) as perc10", s"percentile_approx( ${col}, 0.5) as perc50",
      s" percentile_approx( ${col}, 0.9) as pers90", s"percentile_approx( ${col}, 0.99) as perc99",
      s"corr(${col},${target} ) as pearson").head()

    //spearman
    val rddX = MyColumn.select(col).na.fill(0.0).rdd.map(_.getDouble(0))
    val rddY = MyColumn.select(target).na.fill(0.0).rdd.map(_.getDouble(0))
    val SpearmanCor = Statistics.corr(rddX, rddY, "spearman")

    val m = new MetadataClass(
      max = queue.getDouble(0),
      min = queue.getDouble(1),
      mean = queue.getDouble(2),
      std = queue.getDouble(3),
      dist_value = queue.getLong(4),
      q10 = queue.getDouble(5),
      median = queue.getDouble(6),
      q90 = queue.getDouble(7),
      q99 = queue.getDouble(8),
      pearson_cor = queue.getDouble(9),
      spearman_cor = SpearmanCor
    )

    val transformerFromEsimator = new MyTransformer(uid)
      .setEstimatedCol($(estimatedCol))
      .setTargetCol($(targetCol))
      .setMetadataCol(m)
    transformerFromEsimator

  }

  override def copy(extra: ParamMap): MyEstimator = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = schema
}


