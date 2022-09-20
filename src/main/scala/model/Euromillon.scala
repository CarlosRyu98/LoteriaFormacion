package model

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Euromillon {
  val FECHA = "Fecha"
  val NUM1 = "num1"
  val NUM2 = "num2"
  val NUM3 = "num3"
  val NUM4 = "num4"
  val NUM5 = "num5"
  val NSNC = "NSNC"
  val STAR1 = "Star1"
  val STAR2 = "Star2"

  val schema: StructType = StructType(
    Array(StructField(FECHA, StringType, nullable = false),
      StructField(NUM1, IntegerType, nullable = false),
      StructField(NUM2, IntegerType, nullable = false),
      StructField(NUM3, IntegerType, nullable = false),
      StructField(NUM4, IntegerType, nullable = false),
      StructField(NUM5, IntegerType, nullable = false),
      StructField(NSNC, IntegerType, nullable = true),
      StructField(STAR1, IntegerType, nullable = false),
      StructField(STAR2, IntegerType, nullable = false)
    )
  )
}
