import EstimatorModel.MyParams
import org.apache.spark.ml.{Model, Transformer}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql


object TransformerModel {


  class MyTransformer(override val uid: String) extends Model[MyTransformer]
    with MyParams {
    override def transform(dataset: Dataset[_]): DataFrame = {

      val data: DataFrame = dataset.toDF()
      //add metadata
      val m = $(metadataCol)

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
        .putDouble("cor_spearman", m._spearman_cor)
        .putDouble("distinct", m._dist_value)
        .build()


      val colWithMeta = data.col($(estimatedCol)).as($(estimatedCol), metadata)

      val returnData = data.withColumn($(estimatedCol), colWithMeta)
      // returnData.schema.foreach(field => println(s"${field.name}: metadata=${field.metadata}"))
      returnData
    }


    override def transformSchema(schema: StructType): StructType = schema //

    override def copy(extra: ParamMap): MyTransformer = defaultCopy(extra)
  }

  class DropRainColumns(override val uid: String) extends Transformer {

    override def transform(dataset: Dataset[_]): DataFrame = {
      val result = dataset.drop("RainTomorrow", "RainToday")
      println("Columns RainTomorrow is deleted")
      //result.show()
      result
    }

    override def copy(extra: ParamMap): DropRainColumns = null //defaultCopy(extra)

    override def transformSchema(schema: StructType): StructType = schema
  }

}
