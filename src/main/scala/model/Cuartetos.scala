package model

object Cuartetos {

  //TODO deshacerse de vars
  def calculateFourColumns(columnsInd: Array[String], columnsTrio: Array[String], initialPos: Int = 0): Array[String] = {

    var arrayString: Array[String] = Array()

    for (auxInicial <- initialPos until columnsInd.length - 1) {

      for (auxMid <- auxInicial + 3 until columnsInd.length) {

        arrayString = arrayString :+ columnsTrio(auxInicial) + "," + columnsInd(auxMid)

      }

    }

    arrayString = arrayString :+ columnsTrio(3) + "," + columnsInd(4)
    arrayString = arrayString :+ columnsTrio(6) + "," + columnsInd(4)

    arrayString
  }

}
