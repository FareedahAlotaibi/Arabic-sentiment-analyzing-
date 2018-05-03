import com.diorsding.spark.utils.SentimentUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.parser.ParseException;


import twitter4j.Status;

import scala.Tuple2;

/**
 *
 * This is a e2e test class. Main logic will be integrated in to {@link TwitterStreaming}
 *
 * https://github.com/vspiewak/twitter-sentiment-analysis/blob/master/src/main/scala/com/github/vspiewak/util/SentimentAnalysisUtils.scala
 * https://devpost.com/software/spark-mllib-twitter-sentiment-analysis
 * https://github.com/P7h/Spark-MLlib-Twitter-Sentiment-Analysis/wiki
 *
 *
 * I am not sure if I need to store these analysis data into mysql.
 *
 * @author osydah, joumah
 *
 */
public class TwitterNLP extends TwitterSparkBase {

    public static void main(String[] args) throws IOException, ParseException {
        preSetup();

        nlpAnalyzer();
    }

    private static void nlpAnalyzer() {
        SparkConf sparkConf = new SparkConf().setAppName(TwitterNLP.class.getSimpleName()).setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        String[] filters = { "Trump"};

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc,filters);

        JavaDStream<String> tweets = stream.map(status -> status.getText());

        JavaPairDStream<String, Double> tweetWithScoreDStream =
                tweets.mapToPair(tweetText -> new Tuple2<>(tweetText, Double.valueOf(SentimentUtils.calculateWeightedSentimentScore(tweetText))));


        tweetWithScoreDStream.foreachRDD(rdd -> rdd.foreach(status -> save(status)));
        
        
        
        //tweetWithScoreDStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
    
    private static void save(Tuple2<String, Double> status) throws IOException, SQLException {
        System.out.println("New Tweet");
        System.out.println("Tweet Text :"   + status._1());
        System.out.println("Score :"   + status._2());
        
        PreparedStatement preparedStmt = null;
        Connection conn = null;
        try {

            
            // create a mysql database connection
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(TwitterDBConnectionString.DB_URL, "root", "12345678");
            String query = " insert into tweetswithscore (TweetText,Score,CreatedAt) values (?, ?, Now())";
            // create the mysql insert preparedstatement
            preparedStmt = conn.prepareStatement(query);
            preparedStmt.setString(1, status._1());
            preparedStmt.setString(2, String.valueOf(status._2()));
            // execute the preparedstatement
            preparedStmt.execute();

        } catch (Exception e) {
            System.err.println("Got an exception! " + e);
        }
        finally {
        	preparedStmt.close();
        	conn.close();
        }

    }

}
