
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;
/**
 * Created by liuyuan on 4/7/16.
 */
public class ProfileServlet extends HttpServlet {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "sns";
    private static final String URL = "jdbc:mysql://yuanproject3.cllo0xpf0vni.us-east-1.rds.amazonaws.com/" + DB_NAME;

    private static final String DB_USER = "yuan";
    private static final String DB_PWD = "130602Aa";

    private static Connection conn;

    public ProfileServlet() {
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        /*
            Your initialization code goes here
        */
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        JSONObject result = new JSONObject();

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");

        /*
            Web application will receive a pair of UserID and Password, 
            and the backend checks to see if the UserID and Password is a valid pair. 

            If YES, send back the user's Name and Profile Image URL.
            If NOT, set Name as "Unauthorized" and Profile Image URL as "#".
        */
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String sql = "SELECT name, url FROM user WHERE BINARY `id` = '" + id + "' AND BINARY `password` = '" + pwd + "'";
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                // If there is a match, put acquired information in a Json object
                String name = rs.getString("name");
                String uRL = rs.getString("url");
                result.put("name", name);
                result.put("profile", uRL);
                PrintWriter writer = response.getWriter();
                writer.write(String.format("returnRes(%s)", result.toString()));
                writer.close();
            } else {
                // If input doesn't match with database, put relative information in a Json object
                result.put("name", "Unauthorized");
                result.put("profile", "#");
                PrintWriter writer = response.getWriter();
                writer.write(String.format("returnRes(%s)", result.toString()));
                writer.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}
