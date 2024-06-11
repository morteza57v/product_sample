package net.navoshgaran.mavad.cnv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.navoshgaran.mavad.ConnectionFactory;

public class Preprocessor {
    private String sourceSchema;
    public Preprocessor(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }
    private boolean runDanglingRoleAccess() throws SQLException, ClassNotFoundException {
   	 boolean bool = true;
        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        Connection connection1 = connectionFactory.getConnectionTarget();
        PreparedStatement statement1 = connection1.prepareStatement("select * from " + sourceSchema + ".tbl_module_group where fk_group_id is null or fk_module_id is null");
        ResultSet result1 = statement1.executeQuery();
        if (result1.next()) {
            java.lang.System.out.println("Some of tbl_module_group records have null module_ref or null group_ref!");
            bool = false;
        }
        connectionFactory.closeResultSet(result1);
        connectionFactory.closeStatement(statement1);
        connectionFactory.closeConnection(connection1);
        return bool;
    }
    private boolean runOrphanRoleAccess() throws SQLException, ClassNotFoundException {
        boolean bool = true;
        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        Connection connection1 = connectionFactory.getConnectionTarget();
        PreparedStatement statement1 = connection1.prepareStatement
                ("select * from " + sourceSchema + ".tbl_module_group where fk_group_id not in (select pk_group_id from " + sourceSchema + ".tbl_group) or " +
                 "fk_module_id not in (select pk_module_id from " + sourceSchema + ".tbl_module)");
        ResultSet result1 = statement1.executeQuery();
        if (result1.next()) {

            System.out.println("Some of tbl_module_group records are orphan!");
            bool = false;
        }
        connectionFactory.closeResultSet(result1);
        connectionFactory.closeStatement(statement1);
        connectionFactory.closeConnection(connection1);
        return bool;
    }

    private boolean runDanglingUserRole() throws SQLException, ClassNotFoundException {
   	 boolean bool = true;
        ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
        Connection connection1 = connectionFactory.getConnectionTarget();
        PreparedStatement statement1 = connection1.prepareStatement("select * from " + sourceSchema + ".tbl_user_group where fk_group_id is null");
        ResultSet result1 = statement1.executeQuery();
        if (result1.next()) {
            java.lang.System.out.println("Some of tbl_user_group records have null user_ref or null group_ref!");
            bool = false;
        }
        connectionFactory.closeResultSet(result1);
        connectionFactory.closeStatement(statement1);

        connectionFactory.closeConnection(connection1);
        return bool;
    }

    private boolean runOrphanUserRole() throws SQLException, ClassNotFoundException {

       boolean bool = true;
       ConnectionFactory connectionFactory = ConnectionFactory.getInstance();
       Connection connection1 = connectionFactory.getConnectionTarget();
       PreparedStatement statement1 = connection1.prepareStatement
               ("Select * from " + sourceSchema + ".tbl_user_group where fk_group_id not in (select pk_group_id from " + sourceSchema + ".tbl_group)");
       ResultSet result1 = statement1.executeQuery();
       if (result1.next()) {
           System.out.println("Some of tbl_user_group records are orphan!");
           bool = false;
       }
       connectionFactory.closeResultSet(result1);
       connectionFactory.closeStatement(statement1);
       connectionFactory.closeConnection(connection1);
       return bool;
   }
    
    public boolean run() throws SQLException, ClassNotFoundException {

        boolean b1 = runDanglingRoleAccess();
        boolean b2 = runOrphanRoleAccess();
        boolean b3 = runDanglingUserRole();
        boolean b4 = runOrphanUserRole();
        return b1 && b2 && b3 && b4;
    }

}
