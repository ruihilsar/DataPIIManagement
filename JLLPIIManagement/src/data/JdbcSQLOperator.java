package data;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;


public abstract class JdbcSQLOperator {

	static String DATABASE_HOST = null;

	static String DATABASE_NAME = null;

	static String DATABASE_USER_NAME = null;

	static String DATABASE_PASSWORD = null;
	
	static String DATABASE_DRIVER = null;
	
	static String DATABASE_DRIVER_TYPE = null;

	static String JDBC_URL = null;

	final static String dataSource = "dataSource.properties";

	static {
		if (DATABASE_HOST == null) {
			Properties properties = new Properties();
			try {
//				properties.load(JdbcSQLOperator.class.getClassLoader().getResourceAsStream(dataSource));
				properties.load(new FileInputStream(dataSource));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			DATABASE_DRIVER = properties.getProperty("connection.driver");
			
			if(DATABASE_DRIVER.toLowerCase().contains("sqlserver")) {
				final String url = properties.getProperty("connection.url");
				DATABASE_HOST = url.substring(url.indexOf("jdbc:sqlserver://") + 17, url.indexOf(";databaseName")).trim();
				DATABASE_NAME = url.substring(url.indexOf("databaseName=") + 13).trim().replace(";", "");
				DATABASE_USER_NAME = properties.getProperty("connection.username");
				DATABASE_PASSWORD = properties.getProperty("connection.password");
				JDBC_URL = "jdbc:sqlserver://" + DATABASE_HOST + ";databasename=" + DATABASE_NAME;
			}
			else if(DATABASE_DRIVER.toLowerCase().contains("oracle")) {
				final String url = properties.getProperty("connection.url");
				DATABASE_DRIVER_TYPE = url.substring(0, url.indexOf("@")).trim();
				DATABASE_HOST = url.substring(url.indexOf("@") + 1).trim().replace("//", "").replace(";", "");
				DATABASE_USER_NAME = properties.getProperty("connection.username");
				DATABASE_PASSWORD = properties.getProperty("connection.password");
				JDBC_URL = DATABASE_DRIVER_TYPE + "@" + DATABASE_HOST;
			} else {
				System.out.println("only develop on sql server and oracle database yet, no other DB connection available!");
			}
		}
	}

	protected static Connection connection = null;

	private static Logger logger = Logger.getLogger(JdbcSQLOperator.class.getName());
	
	public static final String getRDBMS(){
		if(DATABASE_DRIVER == null) {
			logger.error("getRDBMS Failed: " + dataSource + " connection driver is incorrect.");
			return null;
		}
		
		if(DATABASE_DRIVER.toLowerCase().contains("sqlserver".toLowerCase())) {
			return "sqlserver";
		}
		else if(DATABASE_DRIVER.toLowerCase().contains("mysql".toLowerCase())) {
			return "mysql";
		}
		else if(DATABASE_DRIVER.toLowerCase().contains("oracle".toLowerCase())) {
			return "oracle";
		}
		else if(DATABASE_DRIVER.toLowerCase().contains("db2".toLowerCase())) {
			return "db2";
		}
		else if(DATABASE_DRIVER.toLowerCase().contains("sybase".toLowerCase())) {
			return "sybase";
		}
		else {
			return null;
		}
	}

	public synchronized static PreparedStatement getPreparedStatement(
			final String sql) throws SQLException {
		try {
			return getPreparedStatement(sql, true);
		} catch (SQLException se) {
			logger.error("Failed: " + sql);
			throw new SQLException(se);
		}
	}

	public synchronized static PreparedStatement getPreparedStatement(
			final String sql, final boolean isLoggeInfo) throws SQLException {
		try {
			if (connection == null || connection.isClosed()) {
				connection = getConn();
			}
			if (isLoggeInfo) {
				logger.info(sql);
			}
			return connection.prepareStatement(sql);
		} catch (SQLException se) {
			logger.error("Failed: " + sql);
			throw new SQLException(se);
		}
	}

	public synchronized static ResultSet executeQuery(
			final PreparedStatement preparedStatement) throws SQLException {
		try {
			return preparedStatement.executeQuery();
		} catch (final SQLException se) {
			// ex.printStackTrace();
			throw new SQLException(se);
		}
	}

	public static final boolean executeSchema(final String sql) throws SQLException {
		try {
			return getPreparedStatement(sql).execute();
		} catch (SQLException se) {
			se.printStackTrace();
			logger.error("Failed:" + sql);
			throw se;
		}
	}
	
	public static final int execute(final String sql) throws SQLException {
		try {
			return getPreparedStatement(sql).executeUpdate();
		} catch (SQLException se) {
			se.printStackTrace();
			logger.error("Failed:" + sql);
			throw se;
		}
	}

	public static final Connection getConn() throws SQLException {
		try {
			return JdbcConnMgr.getConn();
		} catch (SQLException se) {
			// logger.error("ERROR: getConn() :" + e.toString());
			throw se;
		}
	}
	
	public static final void closeConn() throws SQLException {
		try {
			if(connection != null)
				connection.close();
		} catch(SQLException se){
			throw new SQLException(se);
		}
	}
	
}