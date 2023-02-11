import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SklyarovLab03 {
    //Внутри класса
    def main(args: Array[String]): Unit = {
      val spark: SparkSession = SparkSession.builder()
        .config("spark.cassandra.connection.host", "10.0.0.31")
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()

      val clients: DataFrame = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "clients", "keyspace" -> "labdata"))
        .load()
      val visits: DataFrame = spark.read
        .format("org.elasticsearch.spark.sql")
        .options(Map("es.read.metadata" -> "true",
          "es.nodes.wan.only" -> "true",
          "es.port" -> "9200",
          "es.nodes" -> "10.0.0.31",
          "es.net.ssl" -> "false"))
        .load("индекс_Elasticsearch")

      val logs: DataFrame = spark.read
        .json("hdfs:///labs/laba03/weblogs.json")
      def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
        Try {
          new URL(URLDecoder.decode(url, "UTF-8")).getHost
        }.getOrElse("")
      })

      transformedLogs.select(col("uid"), decodeUrlAndGetDomain(col("url")).alias("domain"))
      val cats: DataFrame = spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
        .option("dbtable", "domain_cats")
        .option("user", "pavel_sklyarov")
        .option("password", "I6iTUzqd")
        .option("driver", "org.postgresql.Driver")
        .load()
      result.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://10.0.0.31:5432/postgres") //как в логине к личному кабинету но _ вместо .
        .option("dbtable", "clients")
        .option("user", "pavel_sklyarov")
        .option("password", "I6iTUzqd")
        .option("driver", "org.postgresql.Driver")
        .option("truncate", value = true) //позволит не терять гранты на таблицу
        .mode("overwrite") //очищает данные в таблице перед записью
        .save()
    }
}