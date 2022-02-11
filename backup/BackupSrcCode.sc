/*  ==============================================
 *  ~ Back up repository for unused source code ~
 *  This is to contain any code that may be valuable
 *  for future developement of this program
 *  ============================================== */

    /* declaration of RDDs */
    /* Log: Essentially has similar function as spark.sql
     * Originally had plans to use in scenario 6 but cut due to redundancy
     * saving for potential future use */

    /* Case classes for Mirroring method */
    //  case class bevBranchA(beverage: String, branch: String)
    //  case class bevBranchB(beverage: String, branch: String)
    //  case class bevBranchC(beverage: String, branch: String)
    //  case class bevConscountA(beverage: String, branch: String)
    //  case class bevConscountB(beverage: String, branch: String)
    //  case class bevConscountC(beverage: String, branch: String)

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

    /* Originally on GenSparkData
    * same code found on ScenarioMode
    * removed and placed here due to redundancy */

    //def createDatabase(spark: SparkSession): Unit ={
      /* ~ Create Spark SQL tables - loading Branch & conscount respectively ~ */
      //        spark.sql("create table if not exists bev_branches(beverage String,branch String) row format delimited fields terminated by ','")
      //        spark.sql("create table if not exists bev_conscount(beverage String,conscount Int) row format delimited fields terminated by ','")
      /* ~ Load Bev Branch data into bev_branches ~ */
      //        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE bev_branches")
      //        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE bev_branches")
      //        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE bev_branches")
      /* ~ Load Bev Conscount data into bev_conscount ~ */
      //        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE bev_conscount")
      //        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE bev_conscount")
      //        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE bev_conscount")
    //}

/* Misc */
//    Creation of the SQL data tables from GenSparkData
//    CoffeeShopAnalysis.GenSparkData.createDatabase(spark: SparkSession)
//    CoffeeShopAnalysis.GenSparkData.scenario1(spark: SparkSession)
//    CoffeeShopAnalysis.GenSparkData.scenario2(spark: SparkSession)


//      Testing.P1QueryTEST.scenario1(spark: SparkSession)