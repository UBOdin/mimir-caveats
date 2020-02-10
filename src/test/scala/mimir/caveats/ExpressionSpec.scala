package mimir.caveats

import org.specs2.mutable.Specification
import org.apache.spark.sql.SparkSession

class ExpressionSpec extends Specification
{

  lazy val spark = 
    SparkSession.builder
      .appName("Mimir-Caveat-Test")
      .master("local[*]")
      // .config("spark.sql.catalogImplementation", "hive")
      // .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.driver.extraJavaOptions", s"-Dderby.system.home=$dataDir")
      // .config("spark.sql.warehouse.dir", s"${new File(dataDir).getAbsolutePath}/spark-warehouse")
      // .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${new File(dataDir).getAbsolutePath}/metastore_db;create=true")
      // .registerKryoClasses(SparkUtils.getSparkKryoClasses())
      // .set("spark.kryo.registrator",classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
  lazy val testData = spark.read.csv("test_data/r.csv")

  "Spark" >> {

    "count the lines" >> {

      testData.count() must be equalTo(7)

    }


  }

}