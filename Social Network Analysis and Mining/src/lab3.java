import java.sql.Connection;
import java.sql.DriverManager;

import twitterExtractor.TweetListener2;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class lab3 {
	
	// private static final int minutes = 5000;

	public static void main(String[] args) {

		try{
			Class.forName("com.mysql.jdbc.Driver");
			//System.out.println("he");
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter","root", "77#481122bB?");
			//System.out.println("he");
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.setDebugEnabled(true) .setOAuthConsumerKey("basXIvROD3J1BSAvWOWwxm2Wi")
			.setOAuthConsumerSecret("C1mApGdB2ZDbQzNZ7Sfsj4N6vX5CSPBXGtqAsLh6Q55TlQA0CE")
			.setOAuthAccessToken("109226669-77QGoI2Cqoir71GWJKRlq8RJEAqRC628FARdJ897").setOAuthAccessTokenSecret("0I6DRWcmbz2oF76Socmg6OedRUoar6u0FcD9MtJaceK0j");			
			TwitterStream twittFactory = new TwitterStreamFactory(cb.build()).getInstance();
			TweetListener2 list = new TweetListener2(conn);
			twittFactory.addListener(list);
			twittFactory.sample();
		
		} catch(Exception varible){
			varible.printStackTrace();
		}
	}

}
