import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.Window



object SparkExample {

  def read_from_mysql(_spark: SparkSession, tableName: String): DataFrame = {
    _spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost/Student")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "Aswitha@2004")
      .load()
  }

  def Correlation(data: DataFrame): Unit = {
    val Max_scl = data.groupBy("school").agg(max("G3").alias("max_grade"))
    Max_scl.show()

    val correlationCohabitationAndGrades = data.groupBy("Pstatus").agg(avg("G3").alias("avg_grade_cohabitation"))
    correlationCohabitationAndGrades.show()

    val correlationRomanticAndGrades = data.groupBy("romantic").agg(avg("G3").alias("avg_grade_romantic_relation"))
    correlationRomanticAndGrades.show()

    val performanceByActivity = data.groupBy("activities").agg(avg("G3").alias("avg_grade"))
    performanceByActivity.show()

    val correlationTuitionAndGrades = data.groupBy("paid").agg(avg("G3").alias("avg_grade"))
    correlationTuitionAndGrades.show()

  }

  def compareAverageGrades(studentInfoDF: DataFrame, gradesDF: DataFrame): Unit = {
    // Compare average grades based on school
    val avgGradesBySchool = studentInfoDF
      .join(gradesDF, "student_id")
      .groupBy("school")
      .agg(avg("G3").alias("avg_grade_school"))
    avgGradesBySchool.show()

    // Compare average grades based on sex
    val avgGradesBySex = studentInfoDF
      .join(gradesDF, "student_id")
      .groupBy("sex")
      .agg(avg("G3").alias("avg_grade_sex"))
    avgGradesBySex.show()

    // Compare average grades based on age group
    val avgGradesByAgeGroup = studentInfoDF
      .join(gradesDF, "student_id")
      .withColumn("age_group", when(col("age").between(15, 17), "15-17")
        .when(col("age").between(18, 20), "18-20")
        .otherwise("21-22"))
      .groupBy("age_group")
      .agg(avg("G3").alias("avg_grade_age_group"))
    avgGradesByAgeGroup.show()

    // Compare average grades based on parent cohabitation status
    val avgGradesByAddress = studentInfoDF
      .join(gradesDF, "student_id")
      .groupBy("address")
      .agg(avg("G3").alias("avg_grade_address"))
    avgGradesByAddress.show()

    val avgGradesByInternet = studentInfoDF
      .join(gradesDF, "student_id")
      .groupBy("internet")
      .agg(avg("G3").alias("avg_grade_internet"))
    avgGradesByInternet.show()


  }

  def analyzeGrades(data: DataFrame): DataFrame = {
    val stuWithTotalGrades: DataFrame = data
      .withColumn("total_grades", (col("G1") + col("G2") + col("G3")) / 3)
      .drop("G1", "G2", "G3")

    val maxTotalGrades: Double = stuWithTotalGrades.agg(max("total_grades")).first().getDouble(0)
    val minTotalGrades: Double = stuWithTotalGrades.agg(min("total_grades")).first().getDouble(0)

    val marksUDF = udf((total_grades: Double) => {
      if (total_grades < 7) "low"
      else if (total_grades >= 7 && total_grades < 14) "average"
      else "high"
    })

    val stuWithGrades: DataFrame = stuWithTotalGrades.withColumn("grades", marksUDF(col("total_grades")))

    stuWithGrades.select("school", "total_grades").show()

    val schoolCounts: DataFrame = stuWithGrades.groupBy("school", "grades").count()
    schoolCounts.show()

    val schoolTab1: DataFrame = stuWithGrades.groupBy("school", "grades").count()
    val schoolPerc: DataFrame = schoolTab1
      .withColumn("percentage", col("count") / sum("count").over(Window.partitionBy("school")))
      .orderBy("grades")

    schoolPerc
  }

  def JobGrade(data: DataFrame): DataFrame = {
    //parents job affect the students final grade (G3)
    val jobGradeDF = data.select("Fjob","Mjob" ,"G3")
    val avgGradeByJobDF = jobGradeDF.groupBy("Fjob","Mjob") .agg(
    avg("G3").alias("average_G3"))
    avgGradeByJobDF
  }

  def FailureAlcholoics(data: DataFrame): DataFrame = {
    val totalAlcoholDF = data.withColumn("totalAlcohol", col("Dalc") + col("Walc"))
    val windowSpecFailures = Window.orderBy("failures")
    val resultDF = totalAlcoholDF
      .withColumn("avg_totalAlcohol_failures", avg("totalAlcohol").over(windowSpecFailures))
      .select("failures", "avg_totalAlcohol_failures")
      .distinct()
    resultDF
  }

  def analyzeResourceAllocation(data: DataFrame): DataFrame = {
    val resourceAllocationUDF = udf((g3: Int, failures: Int, health: Int) =>
      if (g3 < 10 && failures >= 2) "High_Tutoring_Needs"
      else if (health < 3) "Counseling_Needs"
      else "No_Additional_Resources_Needed"
    )
    val studentsWithResourceNeeds = data.withColumn(
      "resource_needs",
      resourceAllocationUDF(col("G3"), col("failures"), col("health"))
    )
    studentsWithResourceNeeds
  }



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkExample").master("local[*]").getOrCreate()
    val table1 = "student_info"
    val table2 = "grades"
    val df_info = read_from_mysql(spark, table1)
    val df_grades = read_from_mysql(spark, table2)
    val data = df_info.join(df_grades, "student_id")

    //RISK TO FAIL STUDENTS
    val selectedColumns = Seq(
      "sex", "age", "address", "famsize", "Pstatus", "Medu", "Fedu", "Mjob", "Fjob",
      "traveltime", "studytime", "failures", "schoolsup", "famsup", "paid", "activities",
      "nursery", "higher", "internet", "romantic", "famrel", "freetime", "goout", "Dalc", "Walc", "health", "absences", "G1", "G2", "G3")
    val riskToFailDF = data
      .select(selectedColumns.map(col): _*)
      .withColumn("risk_to_fail", when (col("failures") > 2 || col("absences") > 18, 1).otherwise (0))
    val avgRisk_ToFailDF = riskToFailDF .groupBy("failures", "absences") .agg(avg("risk_to_fail").alias("avg_risk_to_fail"))
    avgRisk_ToFailDF.show()


    //RELATION BETWEEN SCHOOL DISTANCE AND NUMBER OF PASSED
    val enhancedDF = data
      .withColumn("passed", when(col("G3") >= 10, 1).otherwise(0))
      .withColumn("travel_cat", when(col("traveltime") >= 3, "long").otherwise("short"))

    val results = enhancedDF.groupBy("school", "sex", "address", "travel_cat")
      .agg(
        sum("passed").as("num_passed")
      )

    results.groupBy("school","travel_cat").agg(sum("num_passed")).show()

    data.join(results, Seq("school", "sex"))

    //HIGHER EDUCATION
    val higherEducationGradesDF =
      data.groupBy("higher")
        .agg (avg("G3").alias("average_G3")
        )
    higherEducationGradesDF.show()

    //PARENTAL STATUS AND GARDES
    val familyStatusGradeDF = data.select("Pstatus", "G1", "G2", "G3")
    val avgGradesByFamilyStatusDF = familyStatusGradeDF
      .groupBy("Pstatus")
      .agg(
    avg("G3").alias("average_G3"))
    avgGradesByFamilyStatusDF.show()

    //ABESNTEESIM OF STUDENTS BASED ON HEALTH
    val sumAbsencesByHealthDF = data
      .groupBy("health")
      .agg(
    avg("absences").alias("avg_absences"))
    sumAbsencesByHealthDF.show()

    //COMPARING INTERNET ACESS FOR DIFFERENT AREAS
    val internetAddressDF = df_info.select("internet", "address")
    val countByInternetAddressDF = internetAddressDF
      .withColumn("count", count("*").over (Window.partitionBy("internet", "address")))
      .distinct()
    countByInternetAddressDF.show()

    //alcohol consumed by a student on a weekly basis depends upon the family relation level
    val alcohol_FamrelDF = data.select("famrel", "Dalc", "Walc")
    val totalAlcoholDF = alcohol_FamrelDF
  .withColumn("totalAlcohol", col("Dalc") + col("Walc"))
    val windowSpecFamrel = Window.orderBy("famrel")
    val resultDF = totalAlcoholDF
      .withColumn("avg_totalAlcohol_famrel", avg("totalAlcohol").over (windowSpecFamrel))
      .select("famrel",
        "avg_totalAlcohol_famrel")
      .distinct()
    resultDF.show()

    //which exam was easy out of G1, G2 and G3
    val individualAveragesDF = data
      .select("G1", "G2", "G3")
      .agg(avg("G1").alias("avg_G1"), avg("G2").alias("avg_G2"), avg("G3").alias("avg_G3"))
    individualAveragesDF.show()
    val maxIndividualAverage =
    individualAveragesDF
      .selectExpr("GREATEST (avg_G1, avg_G2, avg_G3) as max_individual_average")
      .first()
      .getAs [Double]("max_individual_average")
    println(s"The maximum individual average among grades is: $maxIndividualAverage")

    //relation between number of failures and alcohol intake


    //how mother and father education affect on student final grade (G3)
    val windowSpec = Window. partitionBy("Medu", "Fedu")
    val avgGradesByEducationDF = data
      .withColumn("average_G1", avg("G1").over (windowSpec)) .withColumn("average_G2", avg("G2").over(windowSpec)) .withColumn("average_G3", avg("G3").over (windowSpec))
      .select("Medu", "Fedu", "average_G1", "average_G2", "average_G3") .distinct()
    avgGradesByEducationDF.show()



    //age distribution in each school
    val ageDistributionDF = df_info.groupBy("school", "age") .agg (count("*").alias("count"))
      .orderBy("school", "age")
    ageDistributionDF.show()

    //Functions results
    val result1 = Correlation(data)
    val result3 = compareAverageGrades(df_info,df_grades)
    val result6 = analyzeResourceAllocation(data)
    val result7 = FailureAlcholoics(data)
    val result = JobGrade(data)
    val result5 = analyzeGrades(data)
    spark.stop()

  }
}


