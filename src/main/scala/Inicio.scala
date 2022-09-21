
import com.typesafe.scalalogging.LazyLogging
import commons.sparkSession
import model.Euromillon

object Inicio extends App with sparkSession with LazyLogging {

  val lotoDF = spark.read.option("header", value = true).
    schema(Euromillon.schema).
    csv("src/main/resources/lotoSample.csv").drop(Euromillon.NSNC)

  //Calcular el numero más repetido por columnas y sacar el numero que más veces se repite

  new MaximoColumna().prueba(lotoDF, logger)

  CombinarPares.combinarPares(lotoDF)

}
