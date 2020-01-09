package br.com.bruno.data.ingestion.receitafederal.utils

import br.com.bruno.data.ingestion.receitafederal.bigquery.{CreateDisposition, WriteDisposition}
import org.apache.spark.sql.DataFrame

object WriteData {

  def write(df: DataFrame, tableName: String): Unit = {
    df.saveAsBigQueryTable(tableName, WriteDisposition.WRITE_TRUNCATE, CreateDisposition.CREATE_IF_NEEDED, tmpWriteOptions = Map("recordNamespace" -> "br.com.justto"))
  }

}
