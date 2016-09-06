
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by liuyuan on 4/7/16.
 */

public class RecommendationServlet extends HttpServlet {

	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_NAME = "sns";
	private static final String URL = "jdbc:mysql://yuanproject3.cllo0xpf0vni.us-east-1.rds.amazonaws.com/" + DB_NAME;
	private static final String DB_USER = "yuan";
	private static final String DB_PWD = "130602Aa";
	private static Connection conn;
	
	public RecommendationServlet () throws Exception {
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
			throws ServletException, IOException {

		JSONObject result = new JSONObject();
		String id = request.getParameter("id");

		/**
		 * 
		 * Recommend at most 10 people to the given user with simple collaborative filtering.
		 * 
		 * Results are stored in the result object in the following JSON format:
		 * recommendation: [
		 * 		{name:<name_1>, profile:<profile_1>}
		 * 		{name:<name_2>, profile:<profile_2>}
		 * 		{name:<name_3>, profile:<profile_3>}
		 * 		...
		 * 		{name:<name_10>, profile:<profile_10>}
		 * ]
		 * 
		 */

		Statement stmt1 = null;
		// Maintain a list to store the 2nd degree followees and their edges.
		// Use a "Followee" class to store the userIds and edges of 2nd degree followees
		List<Followee> secondList = new ArrayList<Followee>();
		// Maintain a hash set to store the 1st degree followees and the follower himself.
		HashSet<String> firstSet = new HashSet<String>();
		// Put the follower himself in the hash set.
		firstSet.add(id);
		int count = 0;
		try {
			// Get the 1st degree followees of a follower.
			stmt1 = conn.createStatement();
			String sql = "SELECT followee FROM followerv3 WHERE `follower` = '" + id + "'";
			ResultSet rs = stmt1.executeQuery(sql);
			while (rs.next()) {
				// Put the 1st degree followees in the list.
				String firstFollowee = rs.getString("followee");
				if (!firstSet.contains(firstFollowee)) {
					System.out.println(firstFollowee);
					firstSet.add(firstFollowee);
				}

				// Get the 2nd degree followees of a follower by querying using the userIds of 1st degree followees.
				Statement stmt2 = null;
				try {
					stmt2 = conn.createStatement();
					sql = "SELECT followee FROM followerv3 WHERE `follower` = '" + firstFollowee + "'";

					ResultSet secondRs = stmt2.executeQuery(sql);
					while (secondRs.next()) {
						String secondFollowee = secondRs.getString("followee");
						boolean found = false;
						// If the followee is already in the 2nd degree followee list, then update the edge count of the followee
						for (Followee f : secondList) {
							if (f.getId().equals(secondFollowee)) {
								f.setCount(f.getCount() + 1);
								found = true;
								break;
							}
						}
						// Otherwise, create a new followee instance and put it in the 2nd degree followee list
						if (!found) {
							System.out.println(secondFollowee);
							secondList.add(new Followee(secondFollowee, 1));
						}
					}
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					if (stmt2 != null) {
						try {
							stmt2.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}
				count++;
				System.out.print("------------" + count + "--------------");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt1 != null) {
				try {
					stmt1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		// Delete the 2nd degree followees who are also 1st degree followees
		// If follower himself is also a 2nd degree followee, delete him as well.
		Iterator<Followee> iter = secondList.iterator();
		while(iter.hasNext()){
			if(firstSet.contains(iter.next().getId())) {
				iter.remove();
			}
		}

		// Sort the followees.
		Collections.sort(secondList, new FolloweeComparator());

		// Narrow down list to at most 10 followees.
		List<Followee> thirdList = new ArrayList<Followee>();
		if (secondList.size() > 10) {
			for (int i = 0; i < 10; i++) {
				thirdList.add(secondList.get(i));
			}
		} else {
			thirdList = secondList;
		}

		JSONArray temp = new JSONArray();
		// Query information of the wanted followees.
		for (Followee f : thirdList) {
			Statement stmt3 = null;
			id = f.getId();
			try {
				stmt3 = conn.createStatement();
				String sql = "SELECT name, url FROM user WHERE BINARY `id` = '" + id + "'";
				ResultSet rs = stmt3.executeQuery(sql);
				if (rs.next()) {
					String name = rs.getString("name");
					String uRL = rs.getString("url");
					JSONObject user = new JSONObject();
					user.put("name", name);
					user.put("profile", uRL);
					temp.put(user);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (stmt3 != null) {
					try {
						stmt3.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}

		result.put("recommendation", temp);
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();

	}

	@Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }

	private class Followee {
		private String id;
		private int count;

		public Followee (String id, int count) {
			this.id = id;
			this.count = count;
		}

		public String getId () {
			return id;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
	}

	//
	static class FolloweeComparator implements Comparator<Followee> {
		@Override
		public int compare(Followee f1, Followee f2) {

			// Sort by number of edges in descending order.
			int count1 = f1.getCount();
			int count2 = f2.getCount();

			int compareCount = count2 - count1;

			if (compareCount != 0) {
				return compareCount;
			}

			// Break ties by id in ascending order.
			int id1 = Integer.parseInt(f1.getId());
			int id2 = Integer.parseInt(f2.getId());

			return id1 - id2;
		}
	}
}

