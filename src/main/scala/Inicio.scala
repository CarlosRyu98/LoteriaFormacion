
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

  val pruebaDF = lotoDF.select(Euromillon.NUM1, Euromillon.NUM2, Euromillon.NUM3, Euromillon.NUM4, Euromillon.NUM5)
  val combinatedDF = Functions.addCombinatoryColumn(pruebaDF,pruebaDF.columns.length,2,1)
  val numOfCombinations = Functions.contarCombinaciones(combinatedDF)
  numOfCombinations.show()
}
