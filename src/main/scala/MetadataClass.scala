import org.apache.spark.sql
import org.apache.spark.sql.types.Metadata

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

