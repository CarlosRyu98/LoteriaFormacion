package commons

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Functions {

  // Función para ocntar el número por columnas
  def countNumByColumn(column: String, df: DataFrame): DataFrame = {
    df.groupBy(column)
      .count()
      .withColumnRenamed(column, "num")
  }

  // Función para obtener el número máximo
  def getMaxCountNumber(columns: DataFrame*): DataFrame = {
    columns.reduce(_ union _)
      .groupBy("num")
      .sum("count")
      .sort(desc("sum(count)"))
  }

  // Función que recibe los nombres de las columnas y las devuelve en un Array de columnas
  //TODO deshacerse de vars x
  //TODO cambiar for por foreach x
  //TODO cambiar foreach por map x
  def funcComb(positions: String*): Seq[Column] = {

    val arrayColumns = positions.map(pos =>
      calcularCombinacion(pos)
    ): Seq[Column]

    arrayColumns
  }

  // Quitar el parametro newColumnName
  // Hacer que el parametro position sea de tipo "multyple"
  // Generar nombre de columna
  // Desempaquetar el parametro position introduciendolo en el array
  def calcularCombinacion(positions: String): Column = {
    val pos = positions.split(",")
    val newColumnName = pos.reduce(_ + "_" + _)
    sort_array(
      array(
        pos.map(col): _*
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
