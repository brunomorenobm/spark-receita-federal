package br.com.bruno.data.ingestion.receitafederal.utils

import java.io.File
import java.net.URL

import scala.sys.process._

object FileDownloader {
  def download(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
}
