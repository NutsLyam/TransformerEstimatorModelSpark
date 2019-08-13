
import org.apache.spark.ml.{Model}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}
import org.apache.spark.ml.param.{Param, ParamMap}


class MyTransformer(override val uid: String) extends Model[MyTransformer]
  with MyParams {
  override def transform(dataset: Dataset[_]): DataFrame = {

    val data: DataFrame = dataset.toDF()

    val metadata = $(metadataCol).putMetadata()

    val colWithMeta = data.col($(estimatedCol)).as($(estimatedCol), metadata)

    val returnData = data.withColumn($(estimatedCol), colWithMeta)
    // returnData.schema.foreach(field => println(s"${field.name}: metadata=${field.metadata}"))
    returnData
  }


  override def transformSchema(schema: StructType): StructType = {

    val actualColumnDataType = schema($(estimatedCol)).dataType
    require(actualColumnDataType.equals(DataTypes.DoubleType),
      s"Column ${$(estimatedCol)} must be StringType but was actually $actualColumnDataType .")

    require(schema($(estimatedCol)).metadata.contains("max"),
      s"Column ${$(estimatedCol)}  did not have metadata.")

    //val meta = new MetadataClass().getClassMetadata(field.metadata)

    StructType(schema.fields ++ Seq(StructField("max", DataTypes.DoubleType, true),
      StructField("min", DataTypes.DoubleType, true),
      StructField("mean", DataTypes.DoubleType, true),
      StructField("std", DataTypes.DoubleType, true),
      StructField("quantile_10", DataTypes.DoubleType, true),
      StructField("meduan", DataTypes.DoubleType, true),
      StructField("quantile_90", DataTypes.DoubleType, true),
      StructField("quantile_99", DataTypes.DoubleType, true),
      StructField("cor_pearson", DataTypes.DoubleType, true),
      StructField("cor_pearson", DataTypes.DoubleType, true),
      StructField("distinct", DataTypes.DoubleType, false)))
  }

  override def copy(extra: ParamMap): MyTransformer = defaultCopy(extra)


}
