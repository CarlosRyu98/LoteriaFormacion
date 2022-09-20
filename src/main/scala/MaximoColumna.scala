import com.typesafe.scalalogging.Logger
import commons.Functions.{countNumByColumn, getMaxCountNumber}
import model.Euromillon
import org.apache.spark.sql.DataFrame

class MaximoColumna{

  def prueba(lotoDF: DataFrame, logger: Logger): Unit = {
    val countNum1 = countNumByColumn(Euromillon.NUM1, lotoDF)
    val countNum2 = countNumByColumn(Euromillon.NUM2, lotoDF)
    val countNum3 = countNumByColumn(Euromillon.NUM3, lotoDF)
    val countNum4 = countNumByColumn(Euromillon.NUM4, lotoDF)
    val countNum5 = countNumByColumn(Euromillon.NUM5, lotoDF)

    val countStar1 = countNumByColumn(Euromillon.STAR1, lotoDF)
    val countStar2 = countNumByColumn(Euromillon.STAR2, lotoDF)

    val numOfAppearances = getMaxCountNumber(countNum1, countNum2, countNum3, countNum4, countNum5)
    logger.error("Apariciones de numeros")
    numOfAppearances.show(5)

    val numOfStarAppearances = getMaxCountNumber(countStar2, countStar1)
    logger.error("Apariciones de estrellas")
    numOfStarAppearances.show(5)
  }
}
