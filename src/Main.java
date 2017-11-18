import javafx.util.Pair;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Iarina Dafin on 17/10/24
 */

public class Main {

    private Connection  connection;
    private Statement utility;
    private PreparedStatement join;
    private HashMap<Integer, Float> map = new HashMap<>();
    private ArrayList<Integer> itemsInTestSet = new ArrayList<>();
    private float similarity;
    private HashMap<Integer, HashMap<Integer, ArrayList<Pair<Integer, Pair<Integer, Integer>>>>> itemBased = new HashMap<>();
    //              ^ (item1, item2) as key   ^           ^ user as key AND pair(ratings of item1 and item2) as value
    //                                        ^ list of users that rated both item1 and item2 with their respective ratings

    /**
     * credits go to: sqlitetutorial.net
     * found at: https://sqlite.org/quickstart.html and http://www.sqlitetutorial.net/sqlite-java/create-table/
     *
     * Connects to a database.
     */
    private void connectDatabase () {
        // if on windows
        //String url = "jdbc:sqlite:" + System.getProperty("user.dir").replace('/', '\\') + "\\SQLite\\database.db";
        // if on linux
        //String url = "jdbc:sqlite:" + System.getProperty("user.dir") + "/src/SQLite/" + "database.db";

        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + System.getProperty("user.dir").replace('/', '\\') + "\\SQLite\\database.db");
            utility = connection.createStatement();

            connection.createStatement().execute("ATTACH DATABASE ':memory:' AS d;");
            connection.createStatement().execute("create table d.data (userID integer, itemID integer, rating integer);");
            connection.createStatement().execute("insert into d.data select * from database;");
            connection.createStatement().execute("create index d.user on data (userID);");
            connection.createStatement().execute("create index d.item on data (itemID);");
            connection.createStatement().execute("create index d.useritem on data (userID, itemID);");

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

            utility.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Drops the table and deletes the file in case the active database needs to be recomputed.
     */
    private void dropTable () {
        try {
            utility.execute("DROP TABLE IF EXISTS database;");

            if (new File("SQLite/database.db").delete()) {
                System.out.println("Database file deleted.");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Computes all rating averages of every user.
     */
    private void computeAvgMap(){
        try {
            float f;
            ResultSet res = connection.createStatement().executeQuery("select userID, cast(avg(rating) as real) from d.data group by userID;");
            while (res.next()){
                f = res.getFloat(2);
                map.put(res.getInt(1), f);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void formula (int i, int j) {
        try {
            int nr = 0;
            float average;
            float numarator = 0;
            float sumOfItem1 = 0;
            float sumOfItem2 = 0;
            float item1, item2;

            join.setInt(1, i);
            join.setInt(2, j);
            ResultSet results = join.executeQuery();

            while (results.next()) {
                nr++;
                average = map.get(results.getInt(1));

                item1 = results.getInt(2) - average;
                item2 = results.getInt(3) - average;

                numarator = numarator + (item1 * item2);

                sumOfItem1 = sumOfItem1 + (item1 * item1);
                sumOfItem2 = sumOfItem2 + (item2 * item2);
            }

            if (nr > 1)
                similarity = (float) (numarator / (Math.sqrt((sumOfItem1) * (sumOfItem2))));
            else
                similarity = (float) 6.0;

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes the computation of the similarity matrix.
     * @throws SQLException an exception related to SQL operations
     */
    @SuppressWarnings("unused")
    private void execute() throws SQLException {
        long startTime = System.currentTimeMillis();

        connection.createStatement().execute("delete from itembased;"); // used in case the matrix needs to be recomputed from scratch

        int ee = 0; // initialises the number of the first row that the computation starts on
        // uncomment to enable computing from where you left off
        //PreparedStatement fetchRow = connection.prepareStatement("select * from bktrk limit 1;");
        //int ee = fetchRow.executeQuery().getInt(1);

        computeAvgMap();
        System.out.println("Averages done in: " + ((System.currentTimeMillis() - startTime)/1000) + "s");

        join = connection.prepareStatement("select s.userID, s.rating, p.rating from d.data s inner join d.data p on s.userID = p.userID and s.itemID = ? and p.itemID = ?;");
        join.setFetchSize(5000); // 5000 is the max number of users fetched at once - maybe it makes things slightly faster sometimes?

        PreparedStatement put = connection.prepareStatement("insert into itembased (item1, item2, similarity) values (?, ?, ?);");
        //PreparedStatement put = connection.prepareStatement("update mintest set rating = ? where userID = ? and itemID = ?;"); // only used for updating unknown ratings (which we're currently not doing)

        // prepares statement that updates last row computed
        //PreparedStatement setRow = connection.prepareStatement("update remember set row = ?;");

        // all distinct items are fetched and imported; data is found in the test file table (called mintest)
        // this way, only similarities for items found in the test file will be computed
        long items = System.currentTimeMillis();
        ResultSet get = connection.createStatement().executeQuery("select distinct itemID from mintest;");
        while (get.next())
            itemsInTestSet.add(get.getInt(1));
        System.out.println("Items imported in: " + (System.currentTimeMillis() - items)/1000 + "s");

        // 26383x26383 is the dimension of the similarity matrix
        for (int i = ee; i < 50; ++i){
            long row = System.currentTimeMillis(); // start time of one row
            for (int j = i+1; j < 26383; ++j) {
                //long comp = System.currentTimeMillis(); // start time of one computation
                int a = itemsInTestSet.get(i);
                int b = itemsInTestSet.get(j);

                formula(a, b);

                put.setInt(1, a);
                put.setInt(2, b);

                // if there is no similarity (no users rated both i1 and i2), 6.0 represents a null in the matrix
                // checks if similarity is 6.0 and puts an SQL NULL into the similarity table
                if (similarity == 6.0)
                    put.setNull(3, java.sql.Types.NULL);
                else
                    put.setFloat(3, similarity);

                put.addBatch();

                //System.out.println("Computation " + j + " done in: " + (System.currentTimeMillis() - comp)); // end time of one computation
            }
            put.executeBatch();
            connection.commit();
            put.clearBatch();

            //used if remembering the row is enabled
            //setRow.setInt(1, i+2);
            //setRow.executeUpdate();

            System.out.println("Row " + i + " done in: " + (System.currentTimeMillis() - row)); // end time of one row
        }

        connection.close();

        System.out.println("Ended in: " + (System.currentTimeMillis() - startTime/1000) + " seconds.");
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void importData () {

        try {
            // all distinct items are fetched and imported; data is found in the test file table ! DON'T FORGET to put the min test file into a table called mintest
            // this way, only similarities for items found in the test file will be computed
            long items = System.currentTimeMillis();
            ResultSet get = connection.createStatement().executeQuery("select distinct itemID from mintest order by itemID asc;");
            while (get.next()) {
                itemsInTestSet.add(get.getInt(1));
            }
            System.out.println("Items imported in: " + (System.currentTimeMillis() - items)/1000 + "s");

            PreparedStatement getData = connection.prepareStatement("select s.userID, s.rating, p.rating from d.data s inner join d.data p on s.userID = p.userID and s.itemID = ? and p.itemID = ?;");

            ArrayList<Pair<Integer, Pair<Integer, Integer>>> users;
            HashMap<Integer, ArrayList<Pair<Integer,Pair<Integer,Integer>>>> hm;

            int item1 = 0;
            int item2;
            int c = 0;
            int d = 0;
            for (int i=0; i<26383; i++) {
                hm = new HashMap<>();
                long a = System.currentTimeMillis();
                for (int j = i+1; j<26383; j++) {
                    users = new ArrayList<>();

                    item1 = itemsInTestSet.get(i);
                    item2 = itemsInTestSet.get(j);

                    getData.setInt(1, item1);
                    getData.setInt(2, item2);
                    ResultSet data = getData.executeQuery();

                    while (data.next()) {
                        c++; d++;
                        users.add(new Pair<>(data.getInt(1), new Pair<>(data.getInt(2), data.getInt(3))));
                    }

                    // checks if no users rated both i1 and i2 and does not put anything in the hashmap
                    if (d > 0)
                        hm.put(item2, users);
                    d = 0;
                }
                // checks if no users rated both i1 and i2 and does not put anything in the hashmap
                if (c > 0)
                    itemBased.put(item1, hm);
                c = 0;
                System.out.println("Row " + i + " done in: " + (System.currentTimeMillis() - a));
            }
            System.out.println("Done importing.");

            for (int i = 0; i < 50; i++) {
                for (int j = i + 1; j < 50; j++) {
                    item1 = itemsInTestSet.get(i);
                    item2 = itemsInTestSet.get(j);
                    for (Pair<Integer, Pair<Integer, Integer>> a : itemBased.get(item1).get(item2))
                        System.out.println(item1 + " and " + item2 + ", user: " + a.getKey() + " rating1: " + a.getValue().getKey() + " rating2: " + a.getValue().getValue());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * @param args the command line arguments
     */
    public static void main (String[] args) throws SQLException {
        Main main = new Main();
        main.connectDatabase();
        main.connection.setAutoCommit(false);

        //main.importData();
        main.execute();
    }
}