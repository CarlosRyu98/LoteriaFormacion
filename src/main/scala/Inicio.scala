
import com.typesafe.scalalogging.LazyLogging
import commons.{Functions, sparkSession}
import model.Euromillon

object Inicio extends App with sparkSession with LazyLogging {

  val lotoDF = spark.read.option("header", value = true).
    schema(Euromillon.schema).
    csv("src/main/resources/lotoSample.csv").drop(Euromillon.NSNC)

  //Calcular el numero más repetido por columnas y sacar el numero que más veces se repite

  new MaximoColumna().prueba(lotoDF, logger)

  CombinarPares.combinarPares(lotoDF)

  val columns = Array(
    Euromillon.NUM1,
    Euromillon.NUM2,
    Euromillon.NUM3,
    Euromillon.NUM4,
    Euromillon.NUM5
  )


  //Calcular los pares posibles de cada row
  //  Planificamos las columnas resultantes de combinar las originales
  val columnsArray = Functions.calculateCombinationColumns(
    columns, actualPos = 1)
  //  Obtenemos el df con los pares
  val paresDF = lotoDF.select(columnsArray: _*)
  //  Agrupamos por pares y contamos # apariciones
  val numOfCombinationsAppearances = Functions.contarCombinaciones(paresDF)
  numOfCombinationsAppearances.show()

  //Para obtener las combinaciones relacionadas con las columnas originales realizamos
  // un select uniendo los nombre originales al array de columnas combinadas
  val combinatedDF = lotoDF.select(lotoDF("*") +: columnsArray: _*)
  combinatedDF.show()

  val trios = Functions.calculateThreeColumns(columns)
  val allTrios= lotoDF.select(trios:_*)
  allTrios.show()
  val triosCombinados = Functions.contarCombinaciones(allTrios)
  triosCombinados.show()
}
