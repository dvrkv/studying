package ru.sberbank.bigdata.cloud.arkp.etl.openflow.dm.xssr.wf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ru.sberbank.bigdata.cloud.arkp.etl.meta.valueobjects.CtlStat.{CHANGE, LAST_LOADED_TIME, MAX_PROCESSED_DATE}
import ru.sberbank.bigdata.cloud.arkp.etl.openflow.OpenFlow
import ru.sberbank.bigdata.cloud.arkp.etl.openflow.ops.PropOps._
import ru.sberbank.bigdata.cloud.arkp.etl.openflow.ops.SqlSplitter.RunSQLScripting.{runSQLCompositeScript, p, runningFileName}
import ru.sberbank.bigdata.cloud.arkp.etl.util.Util.getCurrentDateTime

object DataExtractor extends App {
  val f: OpenFlow = OpenFlow(this.getClass.getName)
  import f._
  import propOps._

  // Перечень схем, технологических полей и финальной витрины
  val schemas = Array("prx_uat_xssr_load_mon_npf_self_selfservice_npf", "prx_uat_xssr_load_mon_misuc_self_selfservice_misuc","prx_uat_xssr_load_mon_sfn_self_selfservice_ooosberfondynedvizhimosti_sfn", "prx_uat_xssr_load_mon_broker_self_selfservice_broker", "prx_uat_xssr_load_mon_sbs_self_selfservice_sbs", "prx_uat_xssr_load_mon_sbszh_self_selfservice_sbszh", "prx_uat_xssr_load_mon_npf_self_selfservice_npf_hist", "prx_uat_xssr_load_mon_misuc_self_selfservice_misuc_hist","prx_uat_xssr_load_mon_sfn_self_selfservice_ooosberfondynedvizhimosti_sfn_hist", "prx_uat_xssr_load_mon_broker_self_selfservice_broker_hist", "prx_uat_xssr_load_mon_sbs_self_selfservice_sbs_hist", "prx_uat_xssr_load_mon_sbszh_self_selfservice_sbszh_hist")
  
  val techFields = Array("ctl_loading", "ctl_validfrom", "file_name", "archive_name")
  val tableFact = "custom_fin_xssr_load_mon.t_dzo_replica_load_fact"
  val tableRegistry = "custom_fin_xssr_load_mon_aux.t_dzo_replica_load_registry"

  // Функция возвращает список всех таблиц из всех схем по формату schema.table 
  def tables(spark: SparkSession, schemas: Seq[String]): Seq[String] = {
    val allTables = schemas.flatMap { schema =>
      spark.sql(s"show tables in $schema").collect().map {row => s"$schema.${row.getString(1)}"}
    }
    allTables
  }

  // Функция проверяет наличие в таблице необходимых технологических столбцов
  def checkTechFields(spark: SparkSession, table: String, techFields: Array[String]): Boolean = {
    val tableColumns = spark.sql(s"describe $table").collect().map(_.getString(0))
    techFields.forall(tableColumns.contains)
  }

  // Основная функция сбора данных и вставки в финальные таблицы
  def run(f: OpenFlow): Unit = {

    // Заполнение таблицы t_dzo_replica_load_registry
    val allTables = tables(spark, schemas)
    var legitTablesCount = 0
    println(s"########## Количество всех таблиц из всех схем: ${allTables.size}")
    println(s"########## Началось заполнение таблицы t_dzo_replica_load_registry")

    val dataToInsert_registry: Seq[(String, String, String)] = allTables.map { table =>
      val parts = table.split("\\.")
      val schemaName: String = parts.head.toString
      val tableName = parts.last.toString
      val dzo = if (schemaName.endsWith("_hist")) {
        val schemaLegit = schemaName.stripSuffix("_hist")
        schemaLegit.split("_").last
      } else {
        schemaName.split("_").last
      }

      (schemaName, tableName, dzo)
    }

    import spark.implicits._
    val df = dataToInsert_registry.toDF("schemaName", "tableName", "dzo")
    df.createOrReplaceTempView("temp_registry")

    val table_idDF = spark.read.option("header", "true").csv("/oozie-app/fin/cpubf/custom_fin_xssr_load_mon/sql/dml/idList.csv")
      .withColumn("table_id", col("table_id").cast("bigint"))
    table_idDF.createOrReplaceTempView("df_table_id")

    val count_df_table_id = spark.table("df_table_id").count()
    println(s"########## Количество записей в датафрейме df_table_id: $count_df_table_id")

    spark.sql(s"""
            insert overwrite table $tableRegistry
                select id.table_id, t.schemaName, t.tableName, t.dzo, null
                from temp_registry t
                join df_table_id id
                    on t.schemaName = id.schemaName
                    and t.tableName = id.tableName
                left join $tableRegistry r
                    on t.schemaName = r.schemaName
                    and t.tableName = r.tableName
                    and id.table_id = r.table_id
                where r.schemaName is null
    """)

    println(s"########## Заполнение таблицы t_dzo_replica_load_registry закончилось")
    
    // Заполнение таблицы t_dzo_replica_load_fact
    println(s"########## Началось заполнение таблицы t_dzo_replica_load_fact")

    import f._
    import propOps._

    for (table <- allTables) {
      if (checkTechFields(spark, table, techFields)) {
        legitTablesCount = legitTablesCount + 1
        val parts = table.split("\\.")
        val schemaName: String = parts.head.toString
        val tableName = parts.last.toString
        val tableID = spark.sql(s"""
            select table_id
            from $tableRegistry
            where schema_name = '$schemaName'
            and table_name = '$tableName'""").first().getLong(0)

        // Получение уникальных данных
        val uniqueDataQuery = s"""
            select distinct a.ctl_loading, a.ctl_validfrom, a.file_name, a.archive_name
            from $table as a
            where not exists (select 1 from $tableFact as b
                              where b.ctl_loading = a.ctl_loading
                              and b.ctl_validfrom = a.ctl_validfrom
                              and b.file_name = a.file_name
                              and b.archive_name = a.archive_name)"""

        val uniqueDataDF = spark.sql(uniqueDataQuery)

        // Расчет кол-ва успешно загруженных записей
        val succeeded_count_query = s"""
            select count(*) as succeeded_count, file_name, archive_name, ctl_loading, ctl_validfrom
            from $table as a
            where (file_name, archive_name, ctl_loading, ctl_validfrom) in (select distinct file_name, archive_name, ctl_loading, ctl_validfrom
            from $table as a) as subquery

            --not in
            where not exists (select 1 from $tableFact as b
                              where b.ctl_loading = a.ctl_loading
                              and b.ctl_validfrom = a.ctl_validfrom
                              and b.file_name = a.file_name
                              and b.archive_name = a.archive_name))
                              
            group by file_name, archive_name, ctl_loading, ctl_validfrom"""

        val succeeded_countDF = spark.sql(succeeded_count_query)
        val succeeded_count = succeeded_countDF.collect().head.getLong(0)

        if (!uniqueDataDF.isEmpty) {
          step(s"$p Вставляем финальный набор данных в таблицу $tableFact")

          uniqueDataDF.select(
            lit(tableID).as("table_id"),
            lit(schemaName).as("schema_name"),
            lit(tableName).as("table_name"),
                col("ctl_loading"),
                col("ctl_validfrom"),
                col("file_name"),
                col("archive_name"),
            lit(succeeded_count).as("succeeded_count")
          )
                  .write.insertInto(tableFact)

        } else {
          println(s"########## В таблице $table нет уникальных записей для вставки")
        }

      } else {
        println(s"########## Таблица $table не имеет необходимых технологических столбцов: ${techFields.mkString(", ")}")
      }

    }

    println(s"########## Количество таблиц, в которых есть необходимые технологические столбцы: $legitTablesCount")
    println(s"########## Заполнение таблицы t_dzo_replica_load_fact закончилось")

    val count_fact = spark.sql(s"select count(*) from $tableFact").first().getLong(0)
    val count_registry = spark.sql(s"select count(*) from $tableRegistry").first().getLong(0)
    println(s"########## Количество записей в таблице t_dzo_replica_load_fact: $count_fact")
    println(s"########## Количество записей в таблице t_dzo_replica_load_registry: $count_registry")

  }

  startFlow()

  try {
    run(f = f)
  }
  
  catch {
    case e: NoSuchElementException => stop(e.getMessage)
    case e: Exception              => stop(e.getMessage)
  }

  finishFlow(Map(CHANGE -> "1", LAST_LOADED_TIME -> getCurrentDateTime, MAX_PROCESSED_DATE -> getCurrentDateTime))

}
