package twitterExtractor;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import twitter4j.*;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;



/**
 * Created by IntelliJ IDEA.
 * User: cataldi
 * Date: 3-feb-2010
 * Time: 19.21.19
 * To change this template use File | Settings | File Templates.
 */
public class TweetListener2 implements StatusListener {

    private double numTotalTweets;
    private Connection conn;
    private HashSet<Long> users;
    private static Detector detector;

    public TweetListener2(Connection conn1) {
        conn = conn1;
        numTotalTweets = 0.0;
        users = new HashSet<Long>();

        try {
            DetectorFactory.loadProfile("profiles");


        } catch (Exception e) {
            System.exit(1);
        }

    }


    public void onException(java.lang.Exception ex) {
        ex.printStackTrace();
        System.exit(1);
    }

    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        try {
            System.out.println("----SOSPENSION---");
            Thread.sleep(3600000);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void onScrubGeo(long l, long l1) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void onStallWarning(StallWarning t) {
        try {

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }}

    public void onDeletionNotice(StatusDeletionNotice status) {
    }

    public void onStatus(Status status) {
        try {

            //Thread.sleep(30000);

            //System.out.println("");
            // JSONObject json = new JSONObject(status);
            String tweet = status.getText();
            tweet = tweet.replaceAll("\\,", " ");
            tweet = tweet.replaceAll("\"", " ");
            //tweet = tweet.replaceAll("#","\\#");
            //
            // System.out.println("CULO"+tweet);

            Date dateTweet = status.getCreatedAt();

            User user = status.getUser();
            long idtweet = status.getId();
            long iduser = user.getId();
            Date userCreateAt = user.getCreatedAt();
            int numFollowers = user.getFollowersCount();
            int numFriends = user.getFriendsCount();


            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateTweet1 = sdf.format(dateTweet);
            String userCreatedAt1 = sdf.format(userCreateAt);

            detector = DetectorFactory.create();
            detector.append(tweet);
            String lang = detector.detect();


            if (lang.equals("en")) {
                //System.out.println(tweet);
                Statement stmt = conn.createStatement();
                //String query = "insert into trends (author_id,average,standard_dev, trend, c_value, dependency_coeff) value (" + au.getId() + ",-1,-1,-1,-1," + dc + ")";
                String query = "insert into twitter.tweet (idTweet,text,date,iduser) value (" + idtweet + ",\"" + tweet.trim() + "\",\"" + dateTweet1 + "\"," + iduser + ")";
                // System.out.println(tweet);
                stmt.executeUpdate(query);
                stmt.close();


                if (!users.contains(iduser)) {
                    Statement stmt1 = conn.createStatement();
                    //String query = "insert into trends (author_id,average,standard_dev, trend, c_value, dependency_coeff) value (" + au.getId() + ",-1,-1,-1,-1," + dc + ")";
                    String query1 = "insert into twitter.user (iduser, numFollowers, numFriends, createdAt) value (" + iduser + "," + numFollowers + "," + numFriends + ",\"" + userCreatedAt1 + "\")";
                    //System.out.println(query1);
                    stmt1.executeUpdate(query1);
                    stmt1.close();
                    users.add(iduser);
                }


                numTotalTweets++;
                if (numTotalTweets % 500 == 0) {
                    System.out.println("Numero Tweet: " + numTotalTweets);
                }
                if (numTotalTweets == 1000000) {
                    System.exit(0);
                }
            }
            //UseRecordsFile.writeAuthorInfo(iduser, userCreateAt, numFollowers, numFriends);
            //UseRecordsFile.writeTweetAuthor(idtweet, iduser);
            //UseRecordsFile.writeTweetDate(idtweet, dateTweet);
//                interfaceDb.set("tweet-text", idtweet, tweet);
//                interfaceDb.set("user-date", iduser, userCreateAt.toString());
//                interfaceDb.set("user-numFollowers", iduser, numFollowers);
//                interfaceDb.set("user-numFriends", iduser, numFriends);
//                interfaceDb.set("tweet-author", idtweet, iduser);
//                interfaceDb.set("tweet-date", idtweet, dateTweet.toString());


            //  System.out.println("onStatus got: " + status);
        } catch (Exception e) {

//            e.printStackTrace();

        }
    }

}
