=============================================
Unused Code - Experimental
=============================================

~ RDD -> DF conversion ~
/* 
Branch A following schema from scratch - experimental first attempt
Throws an ArrayOutOfBoundsException

  val schemaString = "beverage branch"
  val fields = schemaString.split("").map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)
  val rowRDD = branchRDDA.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1)))
  val branchADF = spark.createDataFrame(rowRDD, schema)
  branchADF.show()
*/