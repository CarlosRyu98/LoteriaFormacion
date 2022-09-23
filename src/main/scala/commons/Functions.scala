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
      ) :+ calcularCombinacion(col2, col1)

    } else if (initialPos < numColumns - 2) {

      calculateCombinationColumns(
        columns,
        initialPos + 2,
        initialPos + 1
      ) :+ calcularCombinacion(col2, col1)

    } else Array(calcularCombinacion(col2, col1))
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
          arrayColum = arrayColum :+ calcularCombinacion(col1, col2, col3)
        }
      }
    }
    arrayColum
  }

  def calculateThreeColumns2(df: DataFrame, originalDFSize: Int, initialPos: Int = 0): Array[Column] = {

    var arrayColum: Array[Column] = Array()
    //llamar al metodo calculateCombinationColumns y guardar sus datos
    //crear condiciones para añadir nuestro numero delante de las columnas necesarias.
    //Modificar calcularCombinacion para que ahcepte un numero determinado de parametros
    //llamar a calcularCombinacion

    val arrayPares = calculateCombinationColumns(df, df.columns.length, initialPos + 2,initialPos + 1)

    for (num <- initialPos until originalDFSize - 2) {


      var col1: String = df.columns(num)
      var col2: String = df.columns(num + 1)
      var initialMethod = num + 1
      var actualMethod = num + 2

      arrayColum = arrayColum :+ calcularCombinacion(col1, col2)


    }
    arrayColum
  }

  // Quitar el parametro newColumnName
  // Hacer que el parametro position sea de tipo "multyple"
  // Generar nombre de columna
  // Desempaquetar el parametro position introduciendolo en el array
  // DONE
  private def calcularCombinacion(positions: String*) = {
    val newColumnName = positions.reduce(_ + "_" + _)
    sort_array(
      array(
        positions.map(col): _*
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
