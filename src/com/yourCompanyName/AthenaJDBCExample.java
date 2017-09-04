package com.yourCompanyName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.amazonaws.athena.jdbc.AthenaResultSet;
import com.amazonaws.athena.jdbc.AthenaStatementClient;

public class AthenaJDBCExample {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		/*
		 * athenaURL is the connection string used for connection, check what is your region and replace it in the string 
		 * in my case region ="us-east-1" 
		 * if you region is "us-west-2" then your athenaUrl would look something like: 
		 * String athenaUrl = "jdbc:awsathena://athena.us-west-2.amazonaws.com:443/";
		 */

		String athenaUrl = "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/";

		/*
		 * You need to get a new user with aws cli access, once you create this
		 * user you will get access key and a secret key. Replace those in the
		 * variables awsAccessKey and awsSecretKey
		 * 
		 * If you are confused which one is your access key and which one is
		 * secret key, the one which has more characters would be your secret
		 * key
		 */

		String awsAccessKey = "YOURACCESSKEY";
		String awsSecretKey = "YOURSECRETKEY";

		/*
		 * The Amazon S3 location to which your query output is written, for
		 * example s3://query-results-bucket/folder/ Athena would write your
		 * query result in this directory.
		 */
		String s3StagingDir = "s3://athena.staging.dir/";

		/*
		 * Query that you want to execute
		 */
		String query = "show databases";

		Properties athenaProperties = new Properties();
		athenaProperties.put("user", awsAccessKey);
		athenaProperties.put("password", awsSecretKey);
		athenaProperties.put("s3_staging_dir", s3StagingDir);

		Connection conn = null;
		Statement statement = null;

		Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");

		conn = DriverManager.getConnection(athenaUrl, athenaProperties);

		statement = conn.createStatement();

		System.out.println("Running query for user: " + athenaProperties.get("user").toString());

		long startTime = System.nanoTime();
		AthenaResultSet rs = (AthenaResultSet) statement.executeQuery(query);
		Object queryId = ((AthenaStatementClient) rs.getClient()).getQueryExecutionId();
		long duration = (System.nanoTime() - startTime) / 1000000;

		System.out.println("Finished running Query time taken: " + duration + "ms.");

		while (rs.next()) {
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
				System.out.println(rs.getObject(i));
			}
		}

		rs.close();
		conn.close();

	}
}