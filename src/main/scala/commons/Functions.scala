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


  def calculateThreeColumns(columns: Array[String], initialPos: Int = 0): Array[Column] = {

    var arrayColum: Array[Column] = Array()

    for (auxInicial <- initialPos until columns.length - 2) {
      // Cambio de mid
      for (auxMid <- auxInicial + 1 until columns.length - 1) {
        // Cambio de Actual
        for (auxActual <- auxMid + 1 until columns.length) {
          var newColumnName = s"${auxInicial}_${auxMid}_${auxActual}"
          var col1: String = columns(auxInicial)
          var col2: String = columns(auxMid)
          var col3: String = columns(auxActual)
          arrayColum = arrayColum :+ calcularCombinaciontrio(col1, col2, col3, newColumnName)
        }
      }
    }
    arrayColum
  }


  private def calcularCombinacion(actualPos: String, initialPos: String, newColumnName: String) = {
    sort_array(
      array(
        initialPos,
        actualPos
      )
    ).as(newColumnName)
  }

  private def calcularCombinaciontrio(initialPos: String, midPos: String, actualPos: String, newColumnName: String) = {
    sort_array(
      array(
        initialPos,
        midPos,
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
