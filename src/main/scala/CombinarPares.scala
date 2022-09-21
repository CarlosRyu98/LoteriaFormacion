import commons.sparkSession
import model.Euromillon
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array, col, concat, desc, explode, sort_array}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CombinarPares extends App with sparkSession {


  def combinarPares(datos: DataFrame): Unit ={

    //sort_array usado apra ordenar asi nos da igual en que posicion se encuentren los pares que nos vale igual.
    val dfpares = datos
      .select(Euromillon.NUM1,Euromillon.NUM2,Euromillon.NUM3,Euromillon.NUM4,Euromillon.NUM5)
      .withColumn("1_2", sort_array(array(col(Euromillon.NUM1),col(Euromillon.NUM2))))
      .withColumn("1_3", sort_array(array(col(Euromillon.NUM1),col(Euromillon.NUM3))))
      .withColumn("1_4", sort_array(array(col(Euromillon.NUM1),col(Euromillon.NUM4))))
      .withColumn("1_5", sort_array(array(col(Euromillon.NUM1),col(Euromillon.NUM5))))
      .withColumn("2_3", sort_array(array(col(Euromillon.NUM2),col(Euromillon.NUM3))))
      .withColumn("2_4", sort_array(array(col(Euromillon.NUM2),col(Euromillon.NUM4))))
      .withColumn("2_5", sort_array(array(col(Euromillon.NUM2),col(Euromillon.NUM5))))
      .withColumn("3_4", sort_array(array(col(Euromillon.NUM3),col(Euromillon.NUM4))))
      .withColumn("3_5", sort_array(array(col(Euromillon.NUM3),col(Euromillon.NUM5))))
      .withColumn("4_5", sort_array(array(col(Euromillon.NUM4),col(Euromillon.NUM5))))
      .drop(Euromillon.NUM1,Euromillon.NUM2,Euromillon.NUM3,Euromillon.NUM4,Euromillon.NUM5)

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
      .show(5)


  }



}

