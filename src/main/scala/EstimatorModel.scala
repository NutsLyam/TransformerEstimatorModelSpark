import Main.getSparkSession
import TransformerModel.MyTransformer
import org.apache.spark.ml.Estimator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql
import org.apache.spark.sql.types.{Metadata, StructType}


object EstimatorModel {

  class MetadataClass(var _max: Double = 0,
                      var _min: Double = 0,
                      var _mean: Double = 0,
                      var _std: Double = 0,
                      var _dist_value: Long = 0,
                      var _q10: Double = 0,
                      var _median: Double = 0,
                      var _q90: Double = 0,
                      var _q99: Double = 0,
                      var _pearson_cor: Double = 0,
                      var _spearman_cor: Double = 0)


  def getMetadata(m: MetadataClass): sql.types.Metadata = {
    val metadata = new sql.types.MetadataBuilder()
      .putDouble("max", m._max)
      .putDouble("min", m._min)
      .putDouble("mean", m._mean)
      .putDouble("std", m._std)
      .putDouble("quantile_10", m._q10)
      .putDouble("median", m._median)
      .putDouble("quantile_90", m._q90)
      .putDouble("quantile_99", m._q99)
      .putDouble("cor_pearson", m._pearson_cor)
      .putDouble("distinct", m._dist_value)
      .build()
    metadata
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


  }

  class MyEstimator(override val uid: String) extends Estimator[MyTransformer]
    with MyParams {

    override def fit(dataset: Dataset[_]): MyTransformer = {
      val MyColumn = dataset.toDF()
      val target = $(targetCol)
      val col = $(estimatedCol)
      val spark: SparkSession = getSparkSession

      MyColumn.createOrReplaceTempView("Table")
      val sqlDF = spark.sql(s"SELECT MAX(${col}), MIN(${col}), AVG(${col}) as mean, STD(${col}) as std, " +
        s"APPROX_COUNT_DISTINCT(${col}), " +
        s"percentile_approx( ${col}, 0.1) as perc10, percentile_approx( ${col}, 0.5) as perc50," +
        s" percentile_approx( ${col}, 0.9) as pers90, percentile_approx( ${col}, 0.99) as perc99, " +
        s"corr(${col},${target} ) as pearson  FROM Table")
      // sqlDF.show()
      val queue = sqlDF.head()
      //spearman
      val rddX = MyColumn.select(col).na.fill(0.0).rdd.map(_.getDouble(0))
      val rddY = MyColumn.select(target).na.fill(0.0).rdd.map(_.getDouble(0))
      val SpearmanCor = Statistics.corr(rddX, rddY, "spearman")

      val m = new MetadataClass(
        _max = queue.getDouble(0),
        _min = queue.getDouble(1),
        _mean = queue.getDouble(2),
        _std = queue.getDouble(3),
        _dist_value = queue.getLong(4),
        _q10 = queue.getDouble(5),
        _median = queue.getDouble(6),
        _q90 = queue.getDouble(7),
        _q99 = queue.getDouble(8),
        _pearson_cor = queue.getDouble(9),
        _spearman_cor = SpearmanCor
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

}
