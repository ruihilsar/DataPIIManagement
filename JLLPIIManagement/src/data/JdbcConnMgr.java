package data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnMgr {

	private static boolean isInit = false;

	/**
     * 	RDBMS		JDBC driver name								URL format
     	SqlServer	com.microsoft.sqlserver.jdbc.SQLServerDriver	jdbc:sqlserver://hostname;databaseName=
      	MySQL		com.mysql.jdbc.Driver							jdbc:mysql://hostname/ databaseName
		ORACLE		oracle.jdbc.driver.OracleDriver					jdbc:oracle:thin:@hostname:port Number:databaseName
		DB2			COM.ibm.db2.jdbc.net.DB2Driver					jdbc:db2:hostname:port Number/databaseName
		Sybase		com.sybase.jdbc.SybDriver						jdbc:sybase:Tds:hostname: port Number/databaseName
     *
     */
	private static void init() {
		try {
			Class.forName(JdbcSQLOperator.DATABASE_DRIVER);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static final Connection getConn() throws SQLException {
		if (isInit == false) {
			init();
			isInit = true;
		}
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(JdbcSQLOperator.JDBC_URL,
					JdbcSQLOperator.DATABASE_USER_NAME,
					JdbcSQLOperator.DATABASE_PASSWORD);
			System.out.println("url=" + JdbcSQLOperator.JDBC_URL
					+ " \n username=" + JdbcSQLOperator.DATABASE_USER_NAME
					+ "pwd=" + JdbcSQLOperator.DATABASE_PASSWORD);
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
		return conn;
	}

	public static final Connection getSqlServerConn(final String IP,
			final String DBName, final String username, final String pswd) {
		if (isInit == false) {
			init();
			isInit = true;
		}
		Connection conn = null;
		try {
			conn = DriverManager.getConnection("jdbc:sqlserver://" + IP
					+ ";databasename=" + DBName, username, pswd);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
}
