package br.com.bruno.data.ingestion.receitafederal

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** *
  * Create data structure based on documentation: http://200.152.38.155/CNPJ/LAYOUT_DADOS_ABERTOS_CNPJ.pdf
  * https://cnae.ibge.gov.br/images/concla/documentacao/CONCLA-TNJ2018-EstruturaDetalhada.pdf
  *
  * @param spark
  */
class DataStructure(spark: SparkSession) {

  import spark.implicits._

  /**
    * Creates the Company Dataframe
    *
    * @param rawLines
    * @return
    */

  val udfTrim = udf {
    s: String => s.trim
  }

  def createCompanyDataFrame(rawLines: Dataset[String]): DataFrame = {
    val companyRawLines = rawLines.filter(line => line.startsWith("1")).toDF()
    val company = companyRawLines.select(
      udfTrim(companyRawLines("value").substr(1, 1)).alias("type"),
      udfTrim(companyRawLines("value").substr(2, 1)).alias("update_full_indicator"),
      udfTrim(companyRawLines("value").substr(3, 1)).alias("update_type"),
      udfTrim(companyRawLines("value").substr(4, 14)).alias("company_legal_id"),
      udfTrim(companyRawLines("value").substr(18, 1)).alias("company_type"),
      udfTrim(companyRawLines("value").substr(19, 150)).alias("name"),
      udfTrim(companyRawLines("value").substr(169, 55)).alias("alias"),
      udfTrim(companyRawLines("value").substr(224, 2)).alias("status"),
      udfTrim(companyRawLines("value").substr(226, 8)).alias("status_date"),
      udfTrim(companyRawLines("value").substr(234, 2)).alias("status_reason"),
      udfTrim(companyRawLines("value").substr(236, 55)).alias("abroad_city_name"),
      udfTrim(companyRawLines("value").substr(291, 3)).alias("country_code"),
      udfTrim(companyRawLines("value").substr(294, 70)).alias("country_name"),
      udfTrim(companyRawLines("value").substr(364, 4)).alias("legal_code"),
      udfTrim(companyRawLines("value").substr(368, 8)).alias("start_date"),
      udfTrim(companyRawLines("value").substr(376, 7)).alias("activity_code"),
      udfTrim(companyRawLines("value").substr(383, 20)).alias("address_description"),
      udfTrim(companyRawLines("value").substr(403, 60)).alias("address"),
      udfTrim(companyRawLines("value").substr(463, 6)).alias("address_number"),
      udfTrim(companyRawLines("value").substr(469, 156)).alias("address2"),
      udfTrim(companyRawLines("value").substr(625, 50)).alias("district"),
      udfTrim(companyRawLines("value").substr(675, 8)).alias("zipcode"),
      udfTrim(companyRawLines("value").substr(683, 2)).alias("state"),
      udfTrim(companyRawLines("value").substr(685, 4)).alias("city_code"),
      udfTrim(companyRawLines("value").substr(689, 50)).alias("city"),
      udfTrim(companyRawLines("value").substr(739, 12)).alias("phone1"),
      udfTrim(companyRawLines("value").substr(751, 12)).alias("phone2"),
      udfTrim(companyRawLines("value").substr(763, 12)).alias("fax"),
      udfTrim(companyRawLines("value").substr(775, 115)).alias("email"),
      udfTrim(companyRawLines("value").substr(890, 2)).alias("natural_person_qualification"),
      udfTrim(companyRawLines("value").substr(892, 14)).alias("share_capital"),
      udfTrim(companyRawLines("value").substr(906, 2)).alias("company_size"),
      udfTrim(companyRawLines("value").substr(908, 2)).alias("simple_tax_option"),
      udfTrim(companyRawLines("value").substr(909, 8)).alias("simple_tax_option_date"),
      udfTrim(companyRawLines("value").substr(917, 8)).alias("simple_tax_option_exclusion_date"),
      udfTrim(companyRawLines("value").substr(925, 1)).alias("mei_tax_option"),
      udfTrim(companyRawLines("value").substr(926, 23)).alias("special_status"),
      udfTrim(companyRawLines("value").substr(949, 8)).alias("special_status_date")
    )
    company
  }

  def createPartnerDataFrame(rawLines: Dataset[String]): DataFrame = {

    val partnerRawLines = rawLines.filter(line => line.startsWith("2")).toDF()

    val partner = partnerRawLines.select(
      udfTrim(partnerRawLines("value").substr(1, 1)).alias("type"),
      udfTrim(partnerRawLines("value").substr(2, 1)).alias("update_full_indicator"),
      udfTrim(partnerRawLines("value").substr(3, 1)).alias("update_type"),
      udfTrim(partnerRawLines("value").substr(4, 14)).alias("company_legal_id"),
      udfTrim(partnerRawLines("value").substr(18, 1)).alias("person_type"),
      udfTrim(partnerRawLines("value").substr(19, 150)).alias("name"),
      udfTrim(partnerRawLines("value").substr(169, 14)).alias("legal_id"),
      udfTrim(partnerRawLines("value").substr(183, 2)).alias("qualification"),
      udfTrim(partnerRawLines("value").substr(185, 5)).alias("share_value"),
      udfTrim(partnerRawLines("value").substr(190, 8)).alias("start_date"),
      udfTrim(partnerRawLines("value").substr(198, 3)).alias("country_code"),
      udfTrim(partnerRawLines("value").substr(201, 70)).alias("country_name"),
      udfTrim(partnerRawLines("value").substr(271, 11)).alias("agent_legal_id"),
      udfTrim(partnerRawLines("value").substr(282, 60)).alias("agent_name"),
      udfTrim(partnerRawLines("value").substr(342, 2)).alias("agent_qualification")
    )
    partner
  }

  def createActivityCodeDataFrame(rawLines: Dataset[String]): DataFrame = {
    val activityCodeRawLines = rawLines.filter(line => line.startsWith("6")).toDF()

    // Execute UDF Function for every row
    val extractActivities = udf { s: String => s.grouped(7).toList.filter(activityCode => !activityCode.equalsIgnoreCase("0000000")) }

    // Read file format
    val activityCode = activityCodeRawLines.select(
      udfTrim(activityCodeRawLines("value").substr(1, 1)).alias("type"),
      udfTrim(activityCodeRawLines("value").substr(2, 1)).alias("update_full_indicator"),
      udfTrim(activityCodeRawLines("value").substr(3, 1)).alias("update_type"),
      udfTrim(activityCodeRawLines("value").substr(4, 14)).alias("company_legal_id"),
      extractActivities(activityCodeRawLines("value").substr(18, 693)).alias("activity_code")
    )
    activityCode
  }


  // Domain Tables
  def createStatusReasonDataFrame(): DataFrame = {
    val statusReason: DataFrame = createHttpCsvDataFrame("http://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/DominiosMotivoSituaoCadastral.csv")
    statusReason.printSchema()
    statusReason
  }

  def createCnaeTypeDataFrame(): DataFrame = {
    // todo implement
    /*
    val content = scala.io.Source.fromURL("https://cnae.ibge.gov.br/images/concla/downloads/revisao2007/PropCNAE20/CNAE20_EstruturaDetalhada.xls")
    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "true") // Optional, default: false
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
      // .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .load("Worktime.xlsx") */
    null
  }

  def createPartnerTypeDataFrame(): DataFrame = {
    val partnerType: DataFrame = createHttpCsvDataFrame("http://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/DominiosQualificaodoresponsvel.csv")
    partnerType.printSchema()
    partnerType
  }

  private def createHttpCsvDataFrame(url: String) = {
    val content = scala.io.Source.fromURL(url, "ISO8859-1").mkString
    val lines = content.split("\r\n").filter(lines => !lines.startsWith("Código;Descrição")).toSeq.toDS()
    spark
      .read
      .option("mode", "DROPMALFORMED")
      .option("encoding", "ISO8859-1")
      .option("delimiter", ";")
      .option("inferSchema", "false")
      .option("header", "false")
      .schema("code STRING, description STRING")
      .csv(lines)
  }
}
