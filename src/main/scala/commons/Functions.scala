package commons

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Functions {
  def countNumByColumn(column: String, df: DataFrame): DataFrame = {
    df.groupBy(column).count().withColumnRenamed(column, "num")
  }

  def getMaxCountNumber(columns: DataFrame*): DataFrame = {
    columns.reduce(_ union _).groupBy("num").sum("count").sort(desc("sum(count)"))
  }

  def calculateCombinationColumns(columns: Array[String], actualPos: Int, initialPos: Int = 0): Array[Column] = {
    val newColumnName = s"${initialPos + 1}_${actualPos + 1}"
    val col1: String = columns(initialPos)
    val col2: String = columns(actualPos)
    val numColumns = columns.length

    if (actualPos < numColumns - 1) {

      calculateCombinationColumns(
        columns,
        actualPos + 1,
        initialPos
      ) :+ calcularCombinacion(col2, col1, newColumnName)

    } else if (initialPos < numColumns - 2) {

      calculateCombinationColumns(
        columns,
        initialPos + 2,
        initialPos + 1
      ) :+ calcularCombinacion(col2, col1, newColumnName)

    } else Array(calcularCombinacion(col2, col1, newColumnName))
  }


  private def calcularCombinacion(actualPos: String, initialPos: String, newColumnName: String) = {
    sort_array(
      array(
        initialPos,
        actualPos
      )
    ).as(newColumnName)
  }

  def contarCombinaciones(combinadoDF: DataFrame): DataFrame = {
    combinadoDF
      .withColumn("combinacionesTotales", explode(array(combinadoDF.columns.map(combinadoDF(_)): _*)))
      .groupBy("combinacionesTotales")
      .count()
      .orderBy(desc("count"))
  }
}
