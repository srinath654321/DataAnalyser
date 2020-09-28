import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHotelReviewDataRunner {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().master("local[*]")
                .appName("hotel-data-reviewer").getOrCreate();
        Dataset<Row> hotelReviewRatingData = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("mode", "DROPMALFORMED")
                .load("Datafiniti_Hotel_Reviews.csv");
        hotelReviewRatingData = hotelReviewRatingData.withColumnRenamed("reviews.rating", "rating");
        hotelReviewRatingData = hotelReviewRatingData.withColumnRenamed("reviews.userCity", "userCity");
        hotelReviewRatingData = hotelReviewRatingData.withColumnRenamed("reviews.userProvince", "state");
        hotelReviewRatingData = hotelReviewRatingData.withColumn("rating", hotelReviewRatingData.col("rating").cast("int"));
        System.out.println(hotelReviewRatingData.schema());
        totalNumberOfHotelsRated(hotelReviewRatingData);
        averageRatingForEachHotel(hotelReviewRatingData);
        mostUsersFromWhichCity(hotelReviewRatingData);
        mostUsersFromWhichState(hotelReviewRatingData);
        highestRatedHotelFromCity(hotelReviewRatingData, "San Diego");

    }
    private static void highestRatedHotelFromCity(Dataset<Row> hotelReviewRatingData, String city) {
       hotelReviewRatingData =  hotelReviewRatingData.filter(hotelReviewRatingData.col("city").equalTo(city))
                .select("name","rating")
                .groupBy(hotelReviewRatingData.col("name"))
                .avg("rating");

       hotelReviewRatingData.orderBy(hotelReviewRatingData.col("avg(rating)").desc()).show(1);
    }

    private static void mostUsersFromWhichState(Dataset<Row> hotelReviewRatingData) {
        hotelReviewRatingData = hotelReviewRatingData.filter(hotelReviewRatingData.col("state").isNotNull())
                .groupBy("state")
                .count();
        hotelReviewRatingData.orderBy(hotelReviewRatingData.col("count").desc())
                .show(1);
    }

    private static void mostUsersFromWhichCity(Dataset<Row> hotelReviewRatingData) {
        hotelReviewRatingData = hotelReviewRatingData
                .filter(hotelReviewRatingData.col("userCity").isNotNull())
                .groupBy("userCity")
                .count();
        hotelReviewRatingData.orderBy(hotelReviewRatingData.col("count").desc())
                .show(1);
    }

    private static void averageRatingForEachHotel(Dataset<Row> hotelReviewRatingData) {
        hotelReviewRatingData.select(hotelReviewRatingData.col("name"), hotelReviewRatingData.col("rating"))
                .groupBy(hotelReviewRatingData.col("name")).avg("rating")
                .show();
    }

    private static void totalNumberOfHotelsRated(Dataset<Row> hotelReviewRatingData) {
        long totalNumberOfRatedHotels = hotelReviewRatingData
                .filter(hotelReviewRatingData.col("rating").$greater(0))
                .distinct()
                .count();
        System.out.println("Total Number of rated hotels are " + totalNumberOfRatedHotels);
    }
}
