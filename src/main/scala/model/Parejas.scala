package model

object Parejas {

  //TODO deshacerse de vars
  def calculateTwoColumns(columns: Array[String], initialPos: Int = 0): Array[String] = {

    var arrayString: Array[String] = Array()

    for (auxInicial <- initialPos until columns.length) {

      for (auxMid <- auxInicial + 1 until columns.length) {

        arrayString = arrayString :+ columns(auxInicial) + "," + columns(auxMid)

      }

    }

    arrayString
  }

}
