
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.mongodb.ServerAddress;
import org.json.JSONObject;
import org.json.JSONArray;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by liuyuan on 4/7/16.
 */
public class HomepageServlet extends HttpServlet {

    private static MongoDatabase db;
    private static MongoClient mongoClient;

    public HomepageServlet() {
        mongoClient = new MongoClient(new ServerAddress("172.31.5.252", 27017));
        db = mongoClient.getDatabase("Project3_4");
    }

    @Override
    protected void doGet(final HttpServletRequest request,
                         final HttpServletResponse response) throws ServletException, IOException {
        /*
            
            Return all the posts authored by this user. Posts are sorted by Timestamp in ascending order
	     (from the oldest to the latest one).
        */

        String id = request.getParameter("id");
        JSONArray temp = new JSONArray();
        JSONObject result = new JSONObject();
        final ArrayList<JSONObject> list = new ArrayList<JSONObject>();


        // Query data using userId and put results in a list.
        FindIterable<Document> iterable = db.getCollection("data").find(
                new Document("uid", Integer.parseInt(id)));

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                JSONObject object = new JSONObject(document);
                list.add(object);
            }
        });

        // Sort the list according to timestamp.
        Collections.sort(list, new JsonComparator());

        for (JSONObject j : list) {
            System.out.println(j.toString());
            temp.put(j);
        }
        result.put("posts", temp);

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request,
                          final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    static class JsonComparator implements Comparator<JSONObject> {
        @Override
        public int compare(JSONObject j1, JSONObject j2) {

            String time1 = j1.getString("timestamp");
            String time2 = j2.getString("timestamp");

            return time1.compareTo(time2);
        }
    }
}

