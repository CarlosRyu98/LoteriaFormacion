
import org.apache.spark.sql.SparkSession

object Inicio extends App with sparkSession{
    println("hello World")
  val variable1 = 1
  val variable2 = 2
    println("hello World")
  println(variable2)
  println(variable1)

  val df = spark.read.table("")
  import spark.implicits._
  println(s"variable1:$variable1, variable2:$variable2")

  df.filter($"col1".gt())

  /**
   * hola aqui andamos
   */
  def hola = {}
























  hola
}
