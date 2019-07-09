import EstimatorModel.MyEstimator
import Main.{getSparkSession, readTestData}
import TransformerModel.MyTransformer
import breeze.numerics.abs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class TransformerEstimatorTest extends FunSuite {
  test("myTest") {

    val spark: SparkSession = getSparkSession
    val colName = "data"
    val press = 0.001
    val fileName = "testData.csv"
    val Data: DataFrame = readTestData(spark,fileName).
      select(colName, "target", "max", "min", "mean", "std","quantile_10",
        "median","quantile_90","quantile_99","distinct","cor_pearson","cor_spearman")

    Data.show()
    Data.printSchema()

    val myData = Data.select(colName, "target")

    val max = Data.select("max").head().getDouble(0)
    val min = Data.select("min").head().getDouble(0)
    val mean = Data.select("mean").head().getDouble(0)
    val std = Data.select("std").head().getDouble(0)
    val quantile_10 = Data.select("quantile_10").head().getDouble(0)
    val median = Data.select("median").head().getDouble(0)
    val quantile_90 = Data.select("quantile_90").head().getDouble(0)
    val quantile_99 = Data.select("quantile_99").head().getDouble(0)
    val distinct = Data.select("distinct").head().getDouble(0)
    val cor_pearson = Data.select("cor_pearson").head().getDouble(0)
    val cor_spearman = Data.select("cor_spearman").head().getDouble(0)


    val myTransformer = new MyTransformer("myTransformer")
    val estimator = new MyEstimator("myEstimator")
      .setTargetCol("target")
      .setEstimatedCol(colName)

    val transformer = estimator.fit(myData)

    val myDataWithMeta = transformer.transform(myData)


    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("max") - max) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("min") - min) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("mean") - mean) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("std") - std) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("quantile_10") - quantile_10) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("median") - median) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("quantile_90") - quantile_90) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("quantile_99") - quantile_99) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("distinct") - distinct) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("cor_pearson") - cor_pearson) < press)
    assert(abs(myDataWithMeta.schema(colName).metadata.getDouble("cor_spearman") - cor_spearman) < press)



}

}
