package assessment;
/**
 * Assessment to work on using IMDB data
 *
 * Authors:
 * @author Sandeep B
 */

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class IMDBAssessment {
    private static final String NameBasicsFile = "data/name.basics.tsv/data.tsv";
    private static final String TitleRatingsFile = "data/title.ratings.tsv/data.tsv";
    private static final String TitleAKASFile = "data/title.akas.tsv/data.tsv";
    private static SparkSession spark;

    public static Dataset<Row> top20MoviesWithMin50VotesWithRanking(Dataset<Row> data) {
        data.createOrReplaceTempView("titleratings");
        Float averageNumberOfVotes = Float.parseFloat(spark.sql("select avg(numVotes) from titleratings where numVotes >=50 ").collectAsList().get(0).get(0).toString());
        Dataset<Row>  Top20MoviesWithMin50VotesWithRanking = spark.sql("select tconst,averageRating,numVotes,(numVotes/"+averageNumberOfVotes+") * averageRating as ranking from titleratings where numVotes >=50 order by ranking desc").limit(20);
        return Top20MoviesWithMin50VotesWithRanking;
    }

    public static Dataset<Row> mostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking(Dataset<Row> Top20MoviesWithMin50VotesWithRanking, Dataset<Row> data, Dataset<Row> nameBasics) {
        //Top20MoviesWithMin50VotesWithRanking.createOrReplaceTempView("titleratings");
        //data.createOrReplaceTempView("titleakas");
        Dataset<Row> MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking = Top20MoviesWithMin50VotesWithRanking.join(data,Top20MoviesWithMin50VotesWithRanking.col("tconst").equalTo(data.col("titleId"))).orderBy(col("ranking").desc(),col("ordering").asc()).select("tconst","ranking","ordering","title","region");
        MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking = MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking.groupBy("tconst","ranking").agg(collect_set("title").alias("differentTitles")).orderBy(col("ranking").desc());
        nameBasics = nameBasics.select(col("primaryName"),explode(col("knownForTitles")).as("titleId"));
        MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking = MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking.join(nameBasics, MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking.col("tconst").equalTo(nameBasics.col("titleId")));
        MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking = MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking.groupBy("tconst","ranking","differentTitles").agg(collect_set("primaryName").alias("mostCreditedPersons")).orderBy(col("ranking").desc());
        return MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking;
    }

    public static void main(String[] args) {

        spark = SparkSession.builder().appName("IMDBAssessment").master("yarn-client").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<String> titleRatingsFile = spark.read().textFile(TitleRatingsFile).as(Encoders.STRING());
        Dataset<Row> titleRatings = titleRatingsFile.map((MapFunction<String, Row>) l -> TitleRatings.parseTitleRatings(l),TitleRatings.encoder()).cache();
        Dataset<Row> Top20MoviesWithMin50VotesWithRanking = top20MoviesWithMin50VotesWithRanking(titleRatings);
        Top20MoviesWithMin50VotesWithRanking.show();

        Dataset<String> nameBasicsFile = spark.read().textFile(NameBasicsFile).as(Encoders.STRING());
        Dataset<Row> nameBasics = nameBasicsFile.map((MapFunction<String, Row>) l -> NameBasics.parseNameBasics(l),NameBasics.encoder()).cache();


        Dataset<String> titleAKASFile = spark.read().textFile(TitleAKASFile).as(Encoders.STRING());
        Dataset<Row> titleAKAS = titleAKASFile.map((MapFunction<String, Row>) l -> TitleAKAS.parseTitleAKAS(l),TitleAKAS.encoder()).cache();
        Dataset<Row> MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking = mostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking(Top20MoviesWithMin50VotesWithRanking, titleAKAS, nameBasics);
        MostCreditedDiffTitlesForTop20MoviesWithMin50VotesWithRanking.show(1500,0);


        spark.stop();
    }


}
