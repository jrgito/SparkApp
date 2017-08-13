# MODULE -- POSTINGEST

## SYNOPSIS

  Main module of the Sinfo project. In this module different processes have been migrated. In the next list show this case current:  
  
  [PROCESS]
  
    1. dailyPeople
    2. dailySituations
    3. delete24Months
    4. dischargeDate
    5. horizontalization
    6. mkDaily
    7. mkMonthly
    8. monthlyMovements
    9. monthlyPeople
    10. monthlySituations
    11. renumerations
    12. segmentGlobalClient
    13. specificParticipants
    14. unifiedCorres
    15. vipEnm

## USAGE

  TO PRODUCTION
   
To execute a process you need the next param, to execute in production:

In the actuality the url to the repository in nexus is : http://10.48.238.128:8081/
In the future will be: https://datio.es.nexus/

    ´´´
       {
         "project": "spider-info",
         "application": "postIngest",
         "component": "[PROCESS]",
         "size": "S",
         "processType": "spark",
         "params": {
           "SPARK_JOB_CONF_URL": "http://10.48.238.128:8081/repository/maven-snapshots/com/datiobd/spider-info/PostIngest/[VERSION]/PostIngest-[VERSION]-[FILE-PROCESS-CONFIG].conf",
           "SPARK_JOB_JAR_URL": "http://10.48.238.128:8081/repository/maven-snapshots/com/datiobd/spider-info/PostIngest/[VERSION]/PostIngest-[VERSION]-jar-with-dependencies.jar",
           "SPARK_MAIN_CLASS": "com.datiobd.spider.[PROCESS].Process"
         }
       }
       ´´´


 TO LOCAL 
 
If you needs execute the anyone process, you need to do it in this way:

 In the "Configuration Param IDE" add this file conf of the  Process.
 
  Example path:~/spider-info/PostIngest/src/test/resources/monthlyPeople.conf
 
 In this form, the code use the class "SinfoApp". This parameter of the class all the values ​​that would be needed in the execution of a cluster with Spark.
 In other hand, exists other class denominate Generator. With this class de developer can generate test data. Is data is more necesary to execute the process choose.
    
  ~/spider-info/PostIngest/src/main/scala/com/datiobd/spider/monthlyPeople/Generator.scala
 And your param file is in this example: 
   
   Example path: /spider-info/PostIngest/src/main/resources/monthly-people/monthly-people.conf
 


## FEATURES

Current:
* You may specify partition-fields. In this moment, is a field: [closing_date]

