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
    private HashMap<Integer, Float> map = new HashMap<>();

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

    private float formula (int i, int j) {
        try {
            float average;
            float item1, item2;
            float numarator = 0;
            float sumOfitem1 = 0;
            float sumOfitem2 = 0;

            stat.setInt(1, i);
            stat.setInt(2, j);
            ResultSet results = stat.executeQuery();

            while (results.next()) {
                average = map.get(results.getInt(1));
                item1 = results.getInt(2) - average;
                item2 = results.getInt(3) - average;

                numarator = numarator + (item1 * item2);

                sumOfitem1 = sumOfitem1 + (item1 * item1);
                sumOfitem2 = sumOfitem2 + (item2 * item2);
            }

            return numarator / (((float) Math.sqrt(sumOfitem1)) * ((float) Math.sqrt(sumOfitem2)));

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -2;
    }

    /**
     * @param args the command line arguments
     */
    public static void main (String[] args) throws SQLException {

        long startTime = System.currentTimeMillis();

        Main main = new Main();

        main.connectDatabase();
        main.connection.setAutoCommit(false);
        main.connection.createStatement().execute("delete from itembased;");

        main.computeAvgMap();
        long avg = (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Averages done in: " + avg + "s");

        main.stat = main.connection.prepareStatement("select s.userID, s.rating, p.rating from database s inner join database p where s.userID = p.userID and s.itemID = ? AND p.itemID = ?;");

        PreparedStatement put = main.connection.prepareStatement("insert into itembased (item1, item2, similarity) values (?, ?, ?);");

        float sim;

        long eyyy = System.currentTimeMillis();

        for(int i = 1; i < 100; ++i){
            for(int j = i+1; j<100; ++j) {
                //long naspa = System.currentTimeMillis();
                sim = main.formula(i, j);
                //System.out.println("calc " + (System.currentTimeMillis() - naspa));
                //long simainaspa = System.currentTimeMillis();
                put.setInt(1, i);
                put.setInt(2, j);
                put.setFloat(3, sim);
                put.addBatch();
                //System.out.println("put " + (System.currentTimeMillis() - simainaspa));
            }
        }
        long calc = System.currentTimeMillis() - eyyy;
        System.out.println("Calculations done in: " + calc + "ms");
        put.executeBatch();
        main.connection.commit();
        System.out.println("Matrix imported in: " + (System.currentTimeMillis()- eyyy - calc) + "ms");

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