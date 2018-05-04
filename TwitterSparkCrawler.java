package com.diorsding.spark.twitter;

import java.io.IOException;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TwitterSparkCrawler {

    private static final Logger log = Logger.getLogger(TwitterSparkCrawler.class);

    public static void main(String[] args) throws ConfigurationException {
        TwitterSparkCrawler workflow = new TwitterSparkCrawler();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));

        try {
            workflow.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    final String consumerKey = "xxxxxxxxxxxxxxxxxxxxxxxxx";
    final String consumerSecret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    final String accessToken = "xxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    final String accessTokenSecret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    private void run(CompositeConfiguration conf) throws InterruptedException {
        // Spark conf
        SparkConf sparkConf = new SparkConf().setAppName("TwitterSparkCrawler").setMaster(conf.getString("spark.master"))
                .set("spark.serializer", conf.getString("spark.serializer"));
        
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(conf.getLong("stream.duration")));
        //JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(30000));
         
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
        // Twitter4J
        // IMPORTANT: put keys in twitter4J.properties
        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        // Create twitter stream
        
        //String[] filters = { "#Car" };
        String[] filters = { "#سوريا"};
        //TwitterUtils.createStream(jssc, twitterAuth, filters).print();
        // Start the computation
        
        
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, twitterAuth, filters);
        //stream.filter(x -> x.getGeoLocation() != null).foreachRDD(rdd -> rdd.foreach(status -> save(status)));
        stream.foreachRDD(rdd -> rdd.foreach(status -> save(status)));

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        //jssc.start();
       //jssc.awaitTermination();
    }
    
    private static void save(Status status) throws IOException, SQLException {
        //System.out.println("tweet:" + status);

        System.out.println("New tweet");
        System.out.println("Device:" + status.getSource());
        System.out.println("location:" + status.getUser().getLocation());
        System.out.println("text:" + status.getText());
        
        String device = "";
        if(status.getSource().indexOf("iPhone") > -1)
        {
        	device = "iPhone";
        }
        else if(status.getSource().indexOf("iPad") > -1)
        {
        	device = "iPad";

        }
        else if(status.getSource().indexOf("Android") > -1)
        {
        	device = "Android";
        }
        else
        {
        	device = "Web Browser";
        }
        
        PreparedStatement preparedStmt = null;
        Connection conn = null;
        try {

            
            // create a mysql database connection
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(TwitterDBConnectionString.DB_URL, "root", "12345678");
            String query = " insert into tweetswithanalysis (TweetText,Device,Location,CreatedAt) values (?, ?, ?, Now())";
            // create the mysql insert preparedstatement
            preparedStmt = conn.prepareStatement(query);
            preparedStmt.setString(1, status.getText());
            preparedStmt.setString(2, device);
            preparedStmt.setString(3, status.getUser().getLocation());
            // execute the preparedstatement
            preparedStmt.execute();

        } catch (Exception e) {
            System.err.println("Got an exception!" +e);
        }
        finally {
        	preparedStmt.close();
        	conn.close();
        }

    }
}
