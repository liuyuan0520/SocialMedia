import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by liuyuan on 4/7/16.
 */

public class TimelineServlet extends HttpServlet {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "sns";
    private static final String URL = "jdbc:mysql://yuanproject3.cllo0xpf0vni.us-east-1.rds.amazonaws.com/" + DB_NAME;

    private static final String DB_USER = "yuan";
    private static final String DB_PWD = "130602Aa";

    private static Connection conn;


    private static String zkAddr = "172.31.15.230";
    private static String usersTableName = "users";
    private static String followersTableName = "followers";
    private static HTableInterface followersTable;
    private static HTable usersTable;
    private static HConnection hConn;
    private static byte[] ColFamily = Bytes.toBytes("data");
    private final static Logger logger = Logger.getRootLogger();
    private static Configuration conf;

    private static MongoDatabase db;
    private static MongoClient mongoClient;

    public TimelineServlet() throws Exception {
        /*
            Your initialization code goes here
        */
        logger.setLevel(Level.ERROR);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        try {
            hConn = HConnectionManager.createConnection(conf);
            usersTable = new HTable(conf, Bytes.toBytes(usersTableName));
            followersTable = hConn.getTable(Bytes.toBytes(followersTableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        mongoClient = new MongoClient(new ServerAddress("172.31.5.252", 27017));
        db = mongoClient.getDatabase("Project3_4");
    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        String id = request.getParameter("id");
        byte[] idInBytes = Bytes.toBytes(id);

        /*
            Get the name and profile of the user
            Put them as fields in the result JSON object
        */

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String sql = "SELECT name, url FROM user WHERE BINARY `id` = '" + id + "'";
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                String name = rs.getString("name");
                String uRL = rs.getString("url");
                result.put("name", name);
                result.put("profile", uRL);
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

        /*
            Get the follower name and profiles
            Put them in the result JSON object as one array
        */

        JSONArray hBasetemp = new JSONArray();

        ArrayList<JSONObject> list = new ArrayList<JSONObject>();

        Scan scanFollowers = new Scan();
        byte[] followerCol = Bytes.toBytes("follower");
        byte[] followeeCol = Bytes.toBytes("followee");

        scanFollowers.addColumn(ColFamily, followeeCol);
        scanFollowers.addColumn(ColFamily, followerCol);

        // Create a filter to get the followers of a followee.
        Filter filterFollowers = new SingleColumnValueFilter(ColFamily, followeeCol, CompareFilter.CompareOp.EQUAL, idInBytes);
        scanFollowers.setFilter(filterFollowers);
        scanFollowers.setBatch(10);

        ResultScanner rsFollowers = followersTable.getScanner(scanFollowers);

        for (Result rFollower = rsFollowers.next(); rFollower != null; rFollower = rsFollowers.next()) {
            JSONObject follower = new JSONObject();

            byte[] followerInBytes = rFollower.getValue(ColFamily, followerCol);
            System.out.println(Bytes.toString(followerInBytes));

            // Use the acquired follower ID as row key to query data from the other table (user table)
            Get get = new Get(followerInBytes);
            byte[] nameCol = Bytes.toBytes("name");
            byte[] urlCol = Bytes.toBytes("url");

            // Put acquired information in a Json object
            Result rUser = usersTable.get(get);
            String name = Bytes.toString(rUser.getValue(ColFamily, nameCol));
            String url = Bytes.toString(rUser.getValue(ColFamily, urlCol));
            follower.put("name", name);
            follower.put("profile", url);

            // Put each result in the list.
            list.add(follower);
        }
        rsFollowers.close();

        // Sort the list according to username and url.
        Collections.sort(list, new HBaseComparator());

        for (JSONObject j : list) {
            System.out.println(j.toString());
            hBasetemp.put(j);
        }
        result.put("followers", hBasetemp);

        /*
  
            Get the 30 LATEST followee posts and put them in the
            result JSON object as one array.

            The posts are be sorted:
            First in ascending timestamp order
            Then numerically in ascending order by their PID (PostID)
	    if there is a tie on timestamp
        */
        final ArrayList<JSONObject> mongoList = new ArrayList<JSONObject>();
        JSONArray mongoTemp = new JSONArray();

        Scan scanFollowees = new Scan();
        scanFollowees.addColumn(ColFamily, followeeCol);
        scanFollowees.addColumn(ColFamily, followerCol);

        // Create a filter to get the followees of a follower.
        Filter filterFollowees = new SingleColumnValueFilter(ColFamily, followerCol, CompareFilter.CompareOp.EQUAL, idInBytes);
        scanFollowees.setFilter(filterFollowees);
        scanFollowees.setBatch(10);

        ResultScanner rsFollowees = followersTable.getScanner(scanFollowees);
        for (Result rFollowee = rsFollowees.next(); rFollowee != null; rFollowee = rsFollowees.next()) {

            byte[] followeeInBytes = rFollowee.getValue(ColFamily, followeeCol);
            int followeeId = Integer.parseInt(Bytes.toString(followeeInBytes));

            // Query posts using the acquired followee userId and put results in a list.
            FindIterable<Document> iterable = db.getCollection("data").find(
                    new Document("uid", followeeId));
            iterable.forEach(new Block<Document>() {
                @Override
                public void apply(final Document document) {
                    JSONObject object = new JSONObject(document);
                    mongoList.add(object);
                }
            });
        }
        rsFollowees.close();

        // If there are less than 30 posts or 30 posts. Sort them in ascending time order.
        if (mongoList.size() <= 30) {
            Collections.sort(mongoList, new MongoAscendingComparator());
            for (JSONObject j : mongoList) {
                System.out.println(j.toString());
                mongoTemp.put(j);
            }
        } else {
            // If there are more than 30 posts.
            // 1. Sort them in descending time order.
            Collections.sort(mongoList, new MongoDescendingComparator());
            ArrayList<JSONObject> finalMongoList = new ArrayList<JSONObject>();

            // 2. Get the latest 30 posts.
            for (int i = 0; i < 30; i++) {
                finalMongoList.add(mongoList.get(i));
            }
            int count = 0;

            // 3. Sort them again in ascending order.
            Collections.sort(finalMongoList, new MongoAscendingComparator());
            for (JSONObject j : finalMongoList) {
                System.out.println(j.toString());
                mongoTemp.put(j);
                count++;
                System.out.println("------" + count + "------");
            }
        }
        result.put("posts", mongoTemp);

        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    static class HBaseComparator implements Comparator<JSONObject> {
        @Override
        public int compare(JSONObject j1, JSONObject j2) {


            String name1 = j1.getString("name");
            String name2 = j2.getString("name");

            // Sort according to username
            int compareName = name1.compareTo(name2);
            if (compareName != 0) {
                return compareName;
            }

            // Break ties according to username
            String url1 = j1.getString("profile");
            String url2 = j2.getString("profile");
            int compareUrl = url1.compareTo(url2);
            return compareUrl;
        }
    }

    // Sort in ascending time order.
    static class MongoAscendingComparator implements Comparator<JSONObject> {
        @Override
        public int compare(JSONObject j1, JSONObject j2) {

            String time1 = j1.getString("timestamp");
            String time2 = j2.getString("timestamp");

            return time1.compareTo(time2);
        }
    }
    // Sort in descending time order.
    static class MongoDescendingComparator implements Comparator<JSONObject> {
        @Override
        public int compare(JSONObject j1, JSONObject j2) {

            String time1 = j1.getString("timestamp");
            String time2 = j2.getString("timestamp");

            return time2.compareTo(time1);
        }
    }
}

