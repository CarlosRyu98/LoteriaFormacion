package commons

import model.CombinarPares
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Functions {
  def countNumByColumn(column: String, df: DataFrame): DataFrame = {
    df.groupBy(column).count().withColumnRenamed(column, "num")
  }

  def getMaxCountNumber(columns: DataFrame*): DataFrame = {
    columns.reduce(_ union _).groupBy("num").sum("count").sort(desc("sum(count)"))
  }


  //todo este es el que estamos usando
  def calculateThreeColumns2(columns: Array[String], initialPos: Int = 0): Array[Column] = {

    var arrayColum: Array[Column] = Array()
    var arrayColum2 = columns
    //llamar al metodo calculateCombinationColumns y guardar sus datos
    //crear condiciones para añadir nuestro numero delante de las columnas necesarias.
    //Modificar calcularCombinacion para que ahcepte un numero determinado de parametros
    //llamar a calcularCombinacion

    println("ArrayColum es igual a: " + arrayColum2.length)
    val arrayPares = CombinarPares.calculateCombinationColumns(columns: Array[String], initialPos + 2, initialPos + 1)

    println(arrayPares.length)
    for (num <- initialPos until arrayPares.length) {
      println("Entro en for" + num)

      if (num == 0) {
        println("entro en if")
        println(s"datos substraidos ${arrayPares(num)}")
      /*  val columA = concat(arrayColum(2),arrayPares(num))
        println(columA)*/
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
