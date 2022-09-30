package model

object Trios {

  // In: 1 Array[String] de individuos y 1 Array[Column] de Parejas
  // Out: Un Array[Columns] de tríos
  //TODO deshacerse de vars
  def calculateThreeColumns(columnsInd: Array[String], columnsPar: Array[String], initialPos: Int = 0): Array[String] = {

    var arrayString: Array[String] = Array()

    for (auxInicial <- initialPos until columnsPar.length) {

      for (auxMid <- auxInicial + 2 until columnsInd.length) {

        arrayString = arrayString :+ columnsPar(auxInicial) + "," + columnsInd(auxMid)

      }

    }

    arrayString = arrayString :+ columnsPar(4) + "," + columnsInd(3)
    arrayString = arrayString :+ columnsPar(5) + "," + columnsInd(4)
    arrayString = arrayString :+ columnsPar(7) + "," + columnsInd(4)

    arrayString
  }

  def calculateThreeColumns2(columns: Array[String], initialPos: Int = 0): Array[String] = {

    var arrayString: Array[String] = Array()

    for (auxInicial <- initialPos until columns.length - 2) {
      // Cambio de mid
      for (auxMid <- auxInicial + 1 until columns.length - 1) {
        // Cambio de Actual
        for (auxActual <- auxMid + 1 until columns.length) {

          arrayString = arrayString :+ columns(auxInicial) + "," + columns(auxMid) + "," + columns(auxActual)

        }

      }

    }
    arrayString
  }

}
