
import com.typesafe.scalalogging.LazyLogging
import commons.{Functions, sparkSession}
import model.{Euromillon, Parejas, Trios, Cuartetos}
import org.apache.spark.sql.Column

object Inicio extends App with sparkSession with LazyLogging {

  val lotoDF = spark.read.option("header", value = true).
    schema(Euromillon.schema).
    csv("src/main/resources/lotoSample.csv").drop(Euromillon.NSNC)

  // Calcular el numero más repetido por columnas y sacar el numero que más veces se repite
  new MaximoColumna().prueba(lotoDF, logger)

  val columns = Array(
    Euromillon.NUM1,
    Euromillon.NUM2,
    Euromillon.NUM3,
    Euromillon.NUM4,
    Euromillon.NUM5
  )

  //Para obtener las combinaciones relacionadas con las columnas originales realizamos
  // un select uniendo los nombre originales al array de columnas combinadas
  val columnsCol = Functions.funcComb(columns: _*)
  val combinatedDF = lotoDF.select(lotoDF("*") +: columnsCol: _*)
  combinatedDF.show(10, truncate = false)

  // Funciones para obtener parejas
  val pares = Parejas.calculateTwoColumns(columns)
  val paresComb = Functions.funcComb(pares: _*)
  val allPares = lotoDF.select(paresComb: _*)
  allPares.show(10, truncate = false)

  // Función que le metes un array de individuales y otro de parejas y te devuelve un array de tríos
  val trios = Trios.calculateThreeColumns(columns, pares)
  val triosCol = Functions.funcComb(trios: _*)
  val allTrios = lotoDF.select(triosCol: _*)
  allTrios.show(10, truncate = false)

  // Función que le metes un array de individuales y otro de trios y te devuelve un array de cuartetos
  val cuartetos = Cuartetos.calculateFourColumns(columns, trios)
  val cuartetosCol = Functions.funcComb(cuartetos: _*)
  val allCuartetos = lotoDF.select(cuartetosCol: _*)
  allCuartetos.show(10, truncate = false)

  // Mostrar todas las combinaciones
  val allCol: Seq[Column] = paresComb ++ triosCol ++ cuartetosCol
  val allTogether = lotoDF.select(lotoDF("*") +: allCol: _*)
  allTogether.show(10, truncate = false)

  // Agrupamos por pares y contamos # apariciones
  val numOfCombinationsAppearances = Functions.contarCombinaciones(allPares)
  numOfCombinationsAppearances.show(10, truncate = false)

}
