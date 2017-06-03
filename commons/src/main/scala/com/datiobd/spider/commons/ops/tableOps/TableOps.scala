package com.datiobd.spider.commons.ops.tableOps
import org.apache.hadoop.conf.Configuration

/**
  * Created by JRGv89 on 19/05/2017.
  */
private[commons] trait TableOps extends TableUpdater with TableDeleter with TableWriter

