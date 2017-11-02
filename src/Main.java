import javafx.util.Pair;

import java.io.File;
import java.sql.*;
import java.util.HashMap;

/**
 * Created by Iarina Dafin on 17/10/24
 */

public class Main {

    private Connection  connection;
    private Statement statement;
    private PreparedStatement stat;
    private HashMap<Pair<Integer, Integer>, Float> similii = new HashMap<>();
    private HashMap<Pair<Integer, Integer>, Float> similuu = new HashMap<>();
    private float[][] simMatrix = new float[5000][5000];
    private HashMap<Integer, Float> map = new HashMap<>();
    private float c1, c2;
    private float average;
    private float numarator;

    /**
     * credits go to: sqlitetutorial.net
     * found at: https://sqlite.org/quickstart.html and http://www.sqlitetutorial.net/sqlite-java/create-table/
     *
     * Connects to a database.
     */
    private void connectDatabase () {
        String url = "jdbc:sqlite:" + System.getProperty("user.dir").replace('/', '\\') + "\\SQLite\\" + "database.db";

        try {
            connection = DriverManager.getConnection(url);
            statement = connection.createStatement();

            System.out.println("Connected.");
            //createDatabase();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Creates a new database if not created already.
     */
    @SuppressWarnings("unused")
    private void createDatabase() {
        try {
            dropTable();

            DatabaseMetaData meta = connection.getMetaData();
            System.out.println("The driver name is " + meta.getDriverName() + ".");
            System.out.println("A new database has been created.");

            // SQL statement for creating a new table
            String sql = "CREATE TABLE IF NOT EXISTS database (\n"
                    + "	userID varchar(255),\n"
                    + "	itemID varchar(255), \n"
                    + "	rating varchar(255)"
                    + ");";

            statement.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Drops the table and deletes the file in case the active database needs to be recomputed.
     */
    private void dropTable () {
        try {
            statement.execute("DROP TABLE IF EXISTS database;");

            if (new File("SQLite/database.db").delete()) {
                System.out.println("Database file deleted.");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private void computeAvgMap(){
        try {
            float f;
            ResultSet res = connection.createStatement().executeQuery("select userID, cast(avg(rating) as real) from database group by userID;");
            while (res.next()){
                f = res.getFloat(2);
                map.put(res.getInt(1), f);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Calculates average of all ratings of one user.
     */
    private float average (int par) {
        if (map.containsKey(par)) {
            float i = map.get(par);
            return map.get(par);
        }
        return -1;
    }

    /**
     * Gets all users that rated two certain given items.
     */
    private float iiformula (int item1, int item2) {
        try {
            //long a = System.currentTimeMillis();
            numarator = -1;
            stat.setInt(1, item1);
            stat.setInt(2, item2);
            ResultSet res = stat.executeQuery();
            //long b =(System.currentTimeMillis() - a);

            //System.out.println("query " + b);

            while (res.next()) {
                numarator = 0;

                average = average(res.getInt(1));

                c1 = res.getInt(2) - average;
                c2 = res.getInt(3) - average;

                numarator += c1 * c2;
            }
            //System.out.println("compute " + (System.currentTimeMillis() - a - b));

            //System.out.println("computation " + (System.currentTimeMillis()-comp)/1000);
            //if (numarator == -1)
                //System.out.println("set is empty for " + item1 + " and " + item2);

            return numarator;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private float uuformula (int user1, int user2) {
        try {
            numarator = -1;
            stat.setInt(1, user1);
            stat.setInt(2, user2);
            ResultSet res = stat.executeQuery();

            while (res.next()) {
                numarator = 0;

                average = average(res.getInt(1));

                c1 = res.getInt(2) - average;
                c2 = res.getInt(3) - average;

                numarator += c1 * c2;
            }

            return numarator;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * @param args the command line arguments
     */
    public static void main (String[] args) throws SQLException {

        long startTime = System.currentTimeMillis();

        Main main = new Main();

        main.connectDatabase();

        main.computeAvgMap();
        long avg = (System.currentTimeMillis() - startTime)/1000;
        System.out.println("averages done " + avg);

        main.stat = main.connection.prepareStatement("select s.userID, s.rating, p.rating from database s inner join database p where s.userID = p.userID and s.itemID = ? AND p.itemID = ?;");

        long statej = (System.currentTimeMillis() - startTime - avg)/1000;
        System.out.println("statement done " + statej);

        for(int i = 1; i < 500; ++i){
            for(int j = i+1; j<500; ++j) {
                main.similii.put(new Pair<>(i, j), main.iiformula(i, j));
            }
            System.out.println("ye: " + (System.currentTimeMillis() - startTime - avg - statej)/1000);
        }

        main.stat = main.connection.prepareStatement("select s.itemID, s.rating, p.rating from database s inner join database p where s.itemID = p.itemID and s.itemID = ? AND p.itemID = ?;");

        main.connection.close();

        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Computed in: " + totalTime/1000 + " seconds.");
        try {
            main.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}