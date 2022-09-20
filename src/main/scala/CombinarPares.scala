import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array, col, concat, desc, explode, sort_array}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CombinarPares extends App with sparkSession {

  val lotSchema = StructType(Array(StructField("Fecha", StringType, true),
    StructField("num1", IntegerType, true),
    StructField("num2", IntegerType, true),
    StructField("num3", IntegerType, true),
    StructField("num4", IntegerType, true),
    StructField("num5", IntegerType, true),
    StructField("NSNC", IntegerType, true),
    StructField("Star1", IntegerType, true),
    StructField("Star2", IntegerType, true)))


    val df = spark.read.schema(lotSchema)
      .option("header", true)
      .csv("Eurolight.csv")
      .drop("NSNC")

  if (Set(1,2) == Set(2,1)) println("El set es igual")

  combinarPares(df)
  def combinarPares(datos: DataFrame): Unit ={

    //sort_array usado apra ordenar asi nos da igual en que posicion se encuentren los pares que nos vale igual.
    val dfpares = datos
      .select("num1","num2","num3","num4","num5")
      .withColumn("1_2", sort_array(array(col("num1"),col("num2"))))
      .withColumn("1_3", sort_array(array(col("num1"),col("num3"))))
      .withColumn("1_4", sort_array(array(col("num1"),col("num4"))))
      .withColumn("1_5", sort_array(array(col("num1"),col("num5"))))
      .withColumn("2_3", sort_array(array(col("num2"),col("num3"))))
      .withColumn("2_4", sort_array(array(col("num2"),col("num4"))))
      .withColumn("2_5", sort_array(array(col("num2"),col("num5"))))
      .withColumn("3_4", sort_array(array(col("num3"),col("num4"))))
      .withColumn("3_5", sort_array(array(col("num3"),col("num5"))))
      .withColumn("4_5", sort_array(array(col("num4"),col("num5"))))
      .drop("num1","num2","num3","num4","num5")
      //.show()*/

   /* val dfpares = datos
      .select("num1","num2","num3","num4","num5")
      .withColumn("1_2", concat(col("num1"),col("num2")))
      .withColumn("1_3", concat(col("num1"),col("num3")))
      .withColumn("1_4", concat(col("num1"),col("num4")))
      .withColumn("1_5", concat(col("num1"),col("num5")))
      .withColumn("2_3", concat(col("num2"),col("num3")))
      .withColumn("2_4", concat(col("num2"),col("num4")))
      .withColumn("2_5", concat(col("num2"),col("num5")))
      .withColumn("3_4", concat(col("num3"),col("num4")))
      .withColumn("3_5", concat(col("num3"),col("num5")))
      .withColumn("4_5", concat(col("num4"),col("num5")))
      .drop("num1","num2","num3","num4","num5")*/
    /*println("TAmaño1= " + dfpares.count())*/
    contarpares(dfpares)

  }

  def contarpares(dfpares: DataFrame): Unit ={

    dfpares
      .withColumn("totalparejas",explode(array(col("1_2"),col("1_3")
        ,col("1_4"),col("1_5"),col("2_3"),col("2_4")
        ,col("2_5"),col("3_4"),col("3_5"),col("4_5"))))
      .groupBy("totalparejas")
      .count()
      .orderBy(desc("count"))
      .show(50,false)

    println("TAmaño= " + dfpares.count())

  }



}

