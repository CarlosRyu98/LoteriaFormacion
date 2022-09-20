import org.apache.spark.sql.SparkSession

trait sparkSession {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("AppLoteria")
    .getOrCreate()
}