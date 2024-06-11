package net.navoshgaran.mavad;


import java.sql.*;

public final class ConnectionFactory {

    private static ConnectionFactory instance = null;
    
    private ConnectionFactory(){
    }
    private static String userName = "anpdeveloper";
    private static String password = "anp";
    private static String serverIP = "192.168.2.201";
    private static String serverSID = "orcl11";
    private static String userNameTarget="MAVAD";
    private static String passwordTarget="123456";
    private static String userNameCamunda="CAMUNDA";
    private static String passwordCamunda="123456";
    private static String serverIpTarget="192.168.2.58";
    private static String serverSIDTarget="orcl12";
    

    public static String getUserNameTarget() {
		return userNameTarget;
	}

	public static void setUserNameTarget(String userNameTarget) {
		ConnectionFactory.userNameTarget = userNameTarget;
	}

	public static String getPasswordTarget() {
		return passwordTarget;
	}

	public static void setPasswordTarget(String passwordTarget) {
		ConnectionFactory.passwordTarget = passwordTarget;
	}

	public static String getServerIpTarget() {
		return serverIpTarget;
	}

	public static void setServerIpTarget(String serverIpTarget) {
		ConnectionFactory.serverIpTarget = serverIpTarget;
	}

	public static String getServerSIDTarget() {
		return serverSIDTarget;
	}

	public static void setServerSIDTarget(String serverSIDTarget) {
		ConnectionFactory.serverSIDTarget = serverSIDTarget;
	}

	public static String getServerIP() {
        return serverIP;
    }

    public static void setServerIP(String serverIP) {
        ConnectionFactory.serverIP = serverIP;
    }

    public static String getServerSID() {
        return serverSID;
    }

    public static void setServerSID(String serverSID) {
        ConnectionFactory.serverSID = serverSID;
    }


    public static void setUserName(String uName) {
        userName = uName;
    }

    public static void setPassword(String pwd) {
        password = pwd;
    }

    synchronized public static ConnectionFactory getInstance(){

        if(instance == null)
            instance = new ConnectionFactory();
        return instance;
    }

    public Connection getConnection() throws ClassNotFoundException, SQLException {

            Class.forName("oracle.jdbc.driver.OracleDriver");
            return DriverManager.getConnection("jdbc:oracle:thin:@"+ serverIP +":1522:" + serverSID, userName, password);
    }
    
    public Connection getConnectionTarget() throws ClassNotFoundException, SQLException {

        Class.forName("oracle.jdbc.driver.OracleDriver");
        return DriverManager.getConnection("jdbc:oracle:thin:@"+ serverIpTarget +":1521:" + serverSIDTarget, userNameTarget, passwordTarget);
//        return DriverManager.getConnection("jdbc:oracle:thin:@"+ serverIP +":1522:" + serverSID, userName, password);
}

    public Connection getConnectioncamunda() throws ClassNotFoundException, SQLException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        return DriverManager.getConnection("jdbc:oracle:thin:@"+ serverIpTarget +":1521:" + serverSIDTarget, userNameCamunda, passwordCamunda);
//        return DriverManager.getConnection("jdbc:oracle:thin:@"+ serverIP +":1522:" + serverSID, userName, password);
    }



    public void closeStatement(Statement stat) throws SQLException {

            if (stat != null) {
                stat.close();
            }
    }

    public void closeResultSet(ResultSet res) throws SQLException {

            if (res != null) {
                res.close();
            }
    }

    public void closeConnection(Connection conn) throws SQLException {

            conn.close();
    }

    public void main(String[] args) {

    }

    public int generatePrimaryKey(Connection con) throws SQLException {
        final String IDGEN_CALL = "SELECT SQ_ID_GEN.NEXTVAL FROM DUAL";
        PreparedStatement idGen = null;
        ResultSet rst;
        try {
            idGen = con.prepareStatement(IDGEN_CALL);
            rst = idGen.executeQuery();
            rst.next();
            return rst.getInt(1);
        } finally {
            closeStatement(idGen);
        }
    }
}
