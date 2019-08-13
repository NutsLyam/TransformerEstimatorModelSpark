import Main.getSparkSession
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

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
