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
    private float[][] simMatrix = new float[5000][5000];
    private HashMap<Integer, Float> map = new HashMap<Integer, Float>();

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

    /**
     * Queries the database one line at a time.
     */
    private void loadData () {
        int count = 0;


        try {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM database LIMIT 1 OFFSET ?;");
            ResultSet results;

            String sql = "CREATE TABLE IF NOT EXISTS test ("
                    + "userID varchar(255), ";

            PreparedStatement preparedStat = connection.prepareStatement("select distinct itemID from database limit 1 offset ?;");
            ResultSet column;

            //creates 5 columns for 5 items
            while (count < 150) {
                preparedStatement.setInt(1, count);

                preparedStat.setInt(1, count);
                column = preparedStat.executeQuery();
                count++;

                sql = sql.concat("'" + column.getInt(1) + "'" + " varchar(255), ");
                // space for any computation
            }
            sql = sql.substring(0, sql.length() - 2) + ");";
            System.out.println(sql);

            statement.execute(sql);

            statement.execute("INSERT INTO test (userID) select distinct userID from database;");

            count = 0;
            while (count < 150) {
                preparedStatement.setInt(1, count);
                results = preparedStatement.executeQuery();
                count++;

                statement.execute("update test set '" + results.getInt(2) + "' = (select rating from database where" +
                        " userID = " + results.getInt(1) + " and itemID = " + results.getInt(2) + ") where userID = " +
                        results.getInt(1) + ";");
            }

            System.out.println("Database queried: " + count + " records.");
        } catch (SQLException e) {
            e.printStackTrace();
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
            return map.get(par);
        }
        return -1;
    }

    /**
     * Gets all users that rated two certain given items.
     */
    private float formula (int item1, int item2) {
        try {
            stat.setInt(1, item1);
            stat.setInt(2, item2);
            ResultSet res = stat.executeQuery();

            float c1, c2;
            float average;
            float numarator = -1;

            while (res.next()) {
                average = average(res.getInt(1));

                c1 = res.getInt(2) - average;

                c2 = res.getInt(3) - average;

                numarator += c1 * c2;
            }
            if (numarator == -1)
                System.out.println("set is empty for " + item1 + " and " + item2);

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

        main.stat = main.connection.prepareStatement("select s.userID, s.rating, p.rating from database s inner join database p where s.userID = p.userID and s.itemID = ? AND p.itemID = ?;");

        main.computeAvgMap();
        System.out.println("averages done " + (System.currentTimeMillis() - startTime)/1000);

        for(int i = 1; i < 5000; ++i){
            for(int j = i+1; j<5000; ++j) {
                System.out.println("i[" + i + "] j[" + j + "]");
                long st = System.currentTimeMillis();
                main.simMatrix[i][j] = main.formula(i, j);
                System.out.println("checked in: " + (System.currentTimeMillis() - st)/1000);
            }
        }
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