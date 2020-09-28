import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkBabyNamesDataRunner {

    private static void findMostRepeatedNameInAllYears(Dataset<Row> dataset, String gender) {
        dataset =  dataset.filter(dataset.col("sex").equalTo(gender));
        dataset = dataset.select(dataset.col("name"), dataset.col("sex"))
                .groupBy("name", "sex").count();
        System.out.println(dataset.orderBy(dataset.col("count").desc(), dataset.col("name").asc())
                .first().toString());
    }

    private static void findMostRepeatedNameInYear(Dataset<Row> dataset, int year) {
        dataset = dataset.filter(dataset.col("year").equalTo(year))
                .groupBy(dataset.col("name")).count();
        dataset.orderBy(dataset.col("count").desc()).show();
    }

    private static void findMostRepeatedNameInEachYear(Dataset<Row> dataset) {
        dataset = dataset.select(dataset.col("year"), dataset.col("name"))
                .groupBy(dataset.col("name"), dataset.col("year")).count();
        dataset.orderBy(dataset.col("count").desc(), dataset.col("year").desc())
                .show();
    }

    private static void getMaleCountInEachYear(Dataset<Row> dataset, String gender) {
        Dataset<Row> genderCountInEachYear = dataset
                .filter(dataset.col("sex").equalTo(gender))
                .groupBy(dataset.col("year"))
                .count()
                .orderBy(dataset.col("year").desc());
        genderCountInEachYear.show();
    }

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().master("local[*]")
                .appName("baby-names-analyser").getOrCreate();
        Dataset<Row> babyNamesData = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("mode", "DROPMALFORMED")
                .load("baby_names_data.csv");
        babyNamesData = babyNamesData.withColumn("year", babyNamesData.col("year").cast("int"));
        babyNamesData = babyNamesData.withColumn("percent", babyNamesData.col("percent").cast("double"));
        babyNamesData.printSchema();
        findMostRepeatedNameInEachYear(babyNamesData);
        findMostRepeatedNameInAllYears(babyNamesData, "boy");
        findMostRepeatedNameInAllYears(babyNamesData, "girl");
        getMaleCountInEachYear(babyNamesData, "boy");
        getMaleCountInEachYear(babyNamesData, "girl");
        findMostRepeatedNameInYear(babyNamesData, 2005);
    }
}
