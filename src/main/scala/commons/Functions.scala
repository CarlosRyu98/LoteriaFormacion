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

  //todo este es el que estamos usando
  def calculateThreeColumns2(columns: Array[String], initialPos: Int = 0): Array[Column] = {

    var arrayColum: Array[Column] = Array()
    //llamar al metodo calculateCombinationColumns y guardar sus datos
    //crear condiciones para añadir nuestro numero delante de las columnas necesarias.
    //Modificar calcularCombinacion para que ahcepte un numero determinado de parametros
    //llamar a calcularCombinacion

    val arrayPares = calculateCombinationColumns(columns: Array[String], initialPos + 2,initialPos + 1)

    println(arrayPares.length)
    for (num <- initialPos until arrayPares.length) {
      println("Entro en for" + num)

      if (num == 0) {
        println("entro en if")
        println(s"datos substraidos ${arrayPares(num)}")
        arrayPares(num)
      }

      // Me gustaria acceder a cada columna en funcion de su nombre.
      /*if(arrayPares.NAME.contents == "2")
        {

          concatenar las columnas

        }*/
    }
      arrayPares
    }

    //


    // Quitar el parametro newColumnName
    // Hacer que el parametro position sea de tipo "multyple"
    // Generar nombre de columna
    // Desempaquetar el parametro position introduciendolo en el array
    // DONE
    def calcularCombinacion(positions: String*) = {
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
