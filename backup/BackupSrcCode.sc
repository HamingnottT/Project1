/*  ==============================================
 *  ~ Back up repository for unused source code ~
 *  This is to contain any code that may be valuable
 *  for future developement of this program
 *  ============================================== */

    /* declaration of RDDs */
    /* Log: Essentially has similar function as spark.sql
     * Originally had plans to use in scenario 6 but cut due to redundancy
     * saving for potential future use */

    /*
   val branchARDD = spark.sparkContext.textFile("input/Bev_BranchA.txt")
   val branchBRDD = spark.sparkContext.textFile("input/Bev_BranchB.txt")
   val branchCRDD = spark.sparkContext.textFile("input/Bev_BranchC.txt")
   val conscountARDD = spark.sparkContext.textFile("input/Bev_BranchA.txt")
   val conscountBRDD = spark.sparkContext.textFile("input/Bev_BranchB.txt")
   val conscountCRDD = spark.sparkContext.textFile("input/Bev_BranchC.txt")

   /* Conversion of RDDs to DFs by Mirroring Method */
   val branchRDDA2 = branchARDD.map(_.split(","))
   val branchADF = branchRDDA2.map(attributes => bevBranchA(attributes(0), attributes(1).trim)).toDF()
//    branchADF.show()

   //    println("+" + ("-" * 49) + "+")
   val branchRDDB2 = branchBRDD.map(_.split(","))
   val branchBDF = branchRDDB2.map(attributes => bevBranchB(attributes(0), attributes(1).trim)).toDF()
//    branchBDF.show()

   //    println("+" + ("-" * 49) + "+")
   val branchRDDC2 = branchCRDD.map(_.split(","))
   val branchCDF = branchRDDC2.map(attributes => bevBranchC(attributes(0), attributes(1).trim)).toDF()
//    branchCDF.show()

//    println("+" + ("-" * 49) + "+")
   val conscountARDD2 = conscountARDD.map(_.split(","))
   val conscountADF = conscountARDD2.map(attributes => bevConscountA(attributes(0), attributes(1).trim)).toDF()
//    conscountADF.show()

//    println("+" + ("-" * 49) + "+")
   val conscountBRDD2 = conscountBRDD.map(_.split(","))
   val conscountBDF = conscountBRDD2.map(attributes => bevConscountB(attributes(0), attributes(1).trim)).toDF()
//    conscountBDF.show()

//    println("+" + ("-" * 49) + "+")
   val conscountCRDD2 = conscountCRDD.map(_.split(","))
   val conscountCDF = conscountCRDD2.map(attributes => bevConscountC(attributes(0), attributes(1).trim)).toDF()
//    conscountCDF.show()

   /* Merge A, B, & C DFs */
   val prebranchDF = branchADF.union(branchBDF)
   val branchDF = prebranchDF.union(branchCDF)
//    branchDF.show()

   val preconscountDF = conscountADF.union(conscountBDF)
   val conscountDF = preconscountDF.union(conscountCDF)
//    conscountDF.show()

   /* Merge conscount with branch */
   val coffeeShopDF = branchDF.union(conscountDF)
   coffeeShopDF.write.json("input/test.json")
   */