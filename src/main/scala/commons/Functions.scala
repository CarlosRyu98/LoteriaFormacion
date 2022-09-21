package commons

import model.Euromillon
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Functions {
  def countNumByColumn(column: String, df: DataFrame): DataFrame = {
    df.groupBy(column).count().withColumnRenamed(column, "num")
  }

  def getMaxCountNumber(columns: DataFrame*): DataFrame = {
    columns.reduce(_ union _).groupBy("num").sum("count").sort(desc("sum(count)"))
  }

  def addCombinatoryColumn(df: DataFrame, originalDFSize: Int, actualPos: Int, initialPos: Int = 0): DataFrame = {
    val newColumnName = s"${initialPos + 1}_${actualPos + 1}"
    if (actualPos < originalDFSize - 1) {
      addCombinatoryColumn(
        df = df.withColumn(
          newColumnName,
          sort_array(
            array(
              df.columns(initialPos),
              df.columns(actualPos)
            )
          )
        ),
        originalDFSize,
        actualPos = actualPos + 1,
        initialPos = initialPos
      )
    } else if (initialPos < originalDFSize - 2) {
      addCombinatoryColumn(
        df = df.withColumn(
          newColumnName,
          sort_array(
            array(
              df.columns(initialPos),
              df.columns(actualPos)
            )
          )
        ),
        originalDFSize,
        actualPos = initialPos + 2,
        initialPos = initialPos + 1
      )
    } else df.withColumn(newColumnName, sort_array(array(df.columns(initialPos), df.columns(actualPos))))
  }


  def contarCombinaciones(combinadoDF: DataFrame): DataFrame = {
    val auxDF = combinadoDF
      .drop(Euromillon.NUM5, Euromillon.NUM4, Euromillon.NUM3, Euromillon.NUM2, Euromillon.NUM1)

    auxDF
      .withColumn("combinacionesTotales", explode(array(auxDF.columns.map(x => auxDF(x)): _*)))
      .groupBy("combinacionesTotales")
      .count()
      .orderBy(desc("count"))
  }

}
