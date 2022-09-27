package model

import commons.Functions.calcularCombinacion
import org.apache.spark.sql.Column

object CombinarTrios {


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

}
