import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;


import org.jruby.RubyProcess;
import org.json.JSONObject;
import org.json.JSONArray;

/**
 * Created by liuyuan on 4/7/16.
 */

public class FollowerServlet extends HttpServlet {

    private static String zkAddr = "172.31.15.230";

    private static String usersTableName = "users";
    private static String followersTableName = "followers";
    private static HTableInterface followersTable;
    private static HTable usersTable;
    private static HConnection conn;
    private static byte[] ColFamily = Bytes.toBytes("data");
    private final static Logger logger = Logger.getRootLogger();
    private static Configuration conf;

    public FollowerServlet() {
        logger.setLevel(Level.ERROR);
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        try {
            conn = HConnectionManager.createConnection(conf);
            usersTable = new HTable(conf, Bytes.toBytes(usersTableName));
            followersTable = conn.getTable(Bytes.toBytes(followersTableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        /*
            Retrive the followers of this user.
            Send back the Name and Profile Image URL of his/her Followers.

            Sort the followers alphabetically in ascending order by Name.
            If there is a tie in the followers name,
	    they are sorted alphabetically by their Profile Image URL in ascending order.
        */

        String id = request.getParameter("id");
        byte[] idInBytes = Bytes.toBytes(id);

        JSONObject followers = new JSONObject();
        JSONArray temp = new JSONArray();

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
        Collections.sort(list, new JsonComparator());

        for (JSONObject j : list) {
            System.out.println(j.toString());
            temp.put(j);
        }
        followers.put("followers", temp);

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", followers.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }

    static class JsonComparator implements Comparator<JSONObject> {
        @Override
        public int compare(JSONObject j1, JSONObject j2) {

            // Sort according to username
            String name1 = j1.getString("name");
            String name2 = j2.getString("name");

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
}


