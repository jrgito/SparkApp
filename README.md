# SparkApp

#### Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
    * [Dependecies](#dependencies)
    * [Building from sources](#building-from-sources)
3. [Getting Started](#getting-started)
    * [Setup requirements](#setup-requirements)
    * [Beginning with SparkApp](#beginning-with-sparkapp)
    * [Configuration](#configuration)
4. [Usage](#usage)
    * [With SparkApp](#with-sparkapp)
    * [With DFOps](#with-dfops)
5. [Examples](#examples)
    * [Tables](#tables)
    * [DFOps](#dfops)
6. [Api - Reference](#api---reference)
7. [Built With](#built-with)
8. [Versioning](#versioning)
9. [Development](#development)
10. [TODO](#todo)

## Overview

Project with the purpose of make easier the use of columnar spark files.

## Installation

##### Dependencies

```
    <dependency>
        <groupId>com.jrgv89</groupId>
        <artifactId>SparkApp</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>
```

##### Building from sources

```
git clone ...
cd SparkApp && mvn clean package
```


## Getting Started

### Setup Requirements

* Git
* Maven
* Spark 2.1

### Beginning with SparkApp

#### Configuration

A typeSafe config file.

```
    sparkApp {
      config {
        name: ""
        spark: {}
        options : {
            hdfs: {}
            logger: {}
            debug:{}
        }
        defaultTable: {}
        tables: []
      }
      app: {}
    }
```

|Key | Type | Info| Required | Default |
|---|---|---|---|---|
|sparkApp.config.name |String|Spark job name|true||
|sparkApp.config.spark|Object\[String,String\]|Spark Job Options|true||
|sparkApp.config.options|Config|App Options|false||
|sparkApp.config.options.hdfs|Config|Hdfs Configuration|false||
|sparkApp.config.options.hdfs.enable|Boolean|Hdfs is enable|false|true|
|sparkApp.config.options.hdfs.properties|Object\[String,String\]|Properties for hdfs configuration|false|{}: new Configuration()|
|sparkApp.config.options.logger|Config|properties for logger. No use for now|false||
|sparkApp.config.options.logger.enable|Boolean|Is logger active|true|true|
|sparkApp.config.options.debug|Boolean|options for debug|false|false|
|sparkApp.config.options.debug.enable|Boolean|is debug enable|true|false|
|sparkApp.config.defaultTable.path|String|file path|true||
|sparkApp.config.defaultTable.format|String|File format <parquet, avro, csv>|true||
|sparkApp.config.defaultTable.mode|String|Mode for write. <overwrite, append...>|true||
|sparkApp.config.defaultTable.pks|Seq\[String\]|Primary keys of file|true||
|sparkApp.config.defaultTable. includePartitionsAsPk|Boolean|if true, takes partition column as pks|true||
|sparkApp.config.defaultTable.sortPks|Boolean|if true, sorts primaryKeys|true||
|sparkApp.config.defaultTable. partitionColumns|Seq\[String\]|partition columns|true||
|sparkApp.config.defaultTable.properties|Object\[String,Any\]|some properties. Mapped as \[String,Any\]|true||
|sparkApp.config.tables|Seq\[Table\]|list of tables (Note)|true||
|sparkApp.app|Config|whatever you want (Note2)|true|{}|

Note: Table has two required properties: name and pks. Any properties are the same as defaultTable. If some property not set, will take defaultTable's property value.
Note2: Will be accessible from child class.

## Usage

#### With SparkApp

```
class Test(path: String) extends SparkApp(path) {
    override def clazz: Class[_] = this.getClass

    override def execute(spark: SparkSession): Int = ???
}

object Test extends App {
    new Test("path/to/config/dummy.conf").start()
}
```

#### With dfOps

```
class Test extends DFOps {
    val spark = SparkSession   ...
}
```

## Examples

#### Tables

* Read a table

```
override def execute(spark: SparkSession): Int = {
    val df = tables("tableName").read(spark)
}

```

* Write a table

```
override def execute(spark: SparkSession): Int = {
    tables("tableName").write(df)
}
```

* Update a table

```
override def execute(spark: SparkSession): Int = {
    tables("tableName").update(df)
}
```


**NOTE**: _updatePartition_ in which data change partition value should be update with **_update_** instead of _updatePartition_

#### DFOps

* Read a table

```
class Test extends DFOps {
    val spark = SparkSession   ...
    val df = readDF(spark, "path", "csv")
}
```

* Write a table

```
class Test extends DFOps {
    val spark = SparkSession   ...
    writeDF(df,"path", "parquet", "overwrite")
}
```

* Update a table

```
class Test extends DFOps {
    val spark = SparkSession   ...
    updateDF(df, "path", "avro")
}
```

## API - Reference

To see more see Javadoc

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags).

## Development

Feel free to do pull request.

## TODO
* Create Examples
* Improve Logger
* Unit Test
* Acceptance Test
* Add reader and writer options
* Review TODOs
* Add SQL Utils for DF and Tables
* More...



