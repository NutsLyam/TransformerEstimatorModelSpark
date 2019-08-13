
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types.{DataTypes, DoubleType, StructField, StructType}


object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = getSparkSession
    val all_collumns: Seq[String] = Seq("MinTemp", "MaxTemp", "Rainfall", "WindGustSpeed"
      , "WindSpeed9am", "WindSpeed3pm", "Humidity9am", "Humidity3pm",
      "Pressure9am", "Pressure3pm", "Cloud9am", "Cloud3pm", "Temp9am", "Temp3pm", "RainTodayIndex", "RISK_MM",
      "RainTomorrowIndex")

    val data = readData(spark).
      select("MinTemp", "MaxTemp", "Rainfall", "WindGustSpeed", "RainToday",
        "RainTomorrow")

    /** "WindSpeed3pm", "Humidity9am", "Humidity3pm",
      * "Pressure9am", "Pressure3pm", "Cloud9am", "Cloud3pm", "Temp9am", "Temp3pm", "RainToday", "RISK_MM",
      * "RainTomorrow")
      * */

    val DataDoubled = readDataToDoubles(spark).
      select("MinTemp", "MaxTemp", "Rainfall", "WindGustSpeed", "RainToday",
        "RainTomorrow")

    val target_col = "RainTomorrowIndex"
    val IndexerToday = new StringIndexer()
      .setInputCol("RainToday")
      .setOutputCol("RainTodayIndex")
      .setHandleInvalid("keep")

    val DataDoubledToday = IndexerToday.
      fit(DataDoubled).
      transform(DataDoubled)

    val IndexerTomorrow = new StringIndexer()
      .setInputCol("RainTomorrow")
      .setOutputCol("RainTomorrowIndex")
      .setHandleInvalid("keep")

    val dataset = IndexerTomorrow.
      fit(DataDoubledToday).
      transform(DataDoubledToday)


    val datasetWithDroppedCol = dataset.
      drop("RainTomorrow", "RainToday")
    //datasetWithDroppedCol.show()
    //datasetWithDroppedCol.printSchema()


    /** *("MY TRANSFORMER") ***/

    var myData = datasetWithDroppedCol
    val myTransformer = new MyTransformer("myTransformer")

    val selectColumns = myData.columns.toSeq
    for (colName <- selectColumns) {
      val estimator = new MyEstimator("myEstimator")
        .setTargetCol("RainTomorrowIndex")
        .setEstimatedCol(colName)


      ////
      val transformer = estimator.fit(myData)
      // column with meta
      myData = transformer.transform(myData)
      println(transformer.transformSchema(myData.schema))

    }

    println("Metadata for all columns")
    myData.schema.foreach(field => println(s"${field.name}: metadata=${field.metadata}"))
    myData.printSchema()

  }


  def getSparkSession: SparkSession = {
    //For windows only: don't forget to put winutils.exe to c:/bin folder
    System.setProperty("hadoop.home.dir", "c:\\hadoop")

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark_SQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def readData(spark: SparkSession): DataFrame = {
    val wheather = spark.read
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      // .csv("C:\\Users\\Nuts\\IdeaProjects\\task2-spark\\data\\data=2018-02-01")
      .csv("C:\\Users\\Nuts\\IdeaProjects\\task2-spark\\data\\weatherAUS.csv")
    wheather
  }

  def readDataToDoubles(spark: SparkSession): DataFrame = {
    val wheather = spark.read
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("C:\\Users\\Nuts\\IdeaProjects\\TransformerEstimatorModelSpark\\src\\main\\data\\weatherAUS.csv")

    import org.apache.spark.sql
    import spark.implicits._

    val DoubleData = wheather
      .withColumn("RISK_MM", $"RISK_MM".cast(sql.types.DoubleType))
      .withColumn("Temp3pm", $"Temp3pm".cast(sql.types.DoubleType))
      .withColumn("Temp9am", $"Temp9am".cast(sql.types.DoubleType))
      .withColumn("Cloud3pm", $"Cloud3pm".cast(sql.types.DoubleType))
      .withColumn("Cloud9am", $"Cloud9am".cast(sql.types.DoubleType))
      .withColumn("Pressure3pm", $"Pressure3pm".cast(sql.types.DoubleType))
      .withColumn("Pressure9am", $"Pressure9am".cast(sql.types.DoubleType))
      .withColumn("Humidity3pm", $"Humidity3pm".cast(sql.types.DoubleType))
      .withColumn("Humidity9am", $"Humidity9am".cast(sql.types.DoubleType))
      .withColumn("WindSpeed3pm", $"WindSpeed3pm".cast(sql.types.DoubleType))
      .withColumn("WindSpeed9am", $"WindSpeed9am".cast(sql.types.DoubleType))
      .withColumn("MinTemp", $"MinTemp".cast(sql.types.DoubleType))
      .withColumn("MaxTemp", $"MaxTemp".cast(sql.types.DoubleType))
      .withColumn("Rainfall", $"Rainfall".cast(sql.types.DoubleType))
      .withColumn("WindGustSpeed", $"WindGustSpeed".cast(sql.types.DoubleType))

    // DoubleData.printSchema()
    //DoubleData.show()

    DoubleData
  }

  def readTestData(spark: SparkSession, name: String): DataFrame = {
    val data = spark.read
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"C:\\Users\\Nuts\\IdeaProjects\\TransformerEstimatorModelSpark\\src\\main\\data\\${name}")
    data
  }

}
