package br.com.bruno.data.ingestion.receitafederal

import br.com.bruno.data.ingestion.receitafederal.utils.WriteData
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Starter extends App {

  if (args.size < 2) {
    throw new Exception("Error initializing job, please use <source-filepath> <target-project>:<target-database>")
  }

  // Read Configuration From Args
  val sourceFilePath = args(0)
  val targetDatabase = args(1)


  // Start Spark Context
  val spark = SparkSession.builder
    .appName("receita-federal")
    //.master("local[*]")
    .getOrCreate()

  import spark.implicits._


  val source: Dataset[String] = spark.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](sourceFilePath).
    mapPartitions(
      _.map(line => new String(line._2.getBytes, 0, line._2.getLength, "ISO-8859-1"))
    ).toDS()

  val ds = new DataStructure(spark)

  val companyStatusReasonDomain: DataFrame = ds.createStatusReasonDataFrame()
  WriteData.write(companyStatusReasonDomain, s"${targetDatabase}.company_status_reason_domain")

  val partnerTypeDomain: DataFrame = ds.createPartnerTypeDataFrame()
  WriteData.write(partnerTypeDomain, s"${targetDatabase}.partner_type_domain")

  //val companyActivityDomain : DataFrame = ds.createCnaeTypeDataFrame()
  //WriteData.write(companyActivityDomain, s"${targetDatabase}.company_activity_domain")

  val companyActivity: DataFrame = ds.createActivityCodeDataFrame(source)
  WriteData.write(companyActivity, s"${targetDatabase}.company_activity")

  val company: DataFrame = ds.createCompanyDataFrame(source)
  WriteData.write(company, s"${targetDatabase}.company")

  val partner: DataFrame = ds.createPartnerDataFrame(source)
  WriteData.write(partner, s"${targetDatabase}.partner")


}
