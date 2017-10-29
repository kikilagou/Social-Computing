import java.io.File;
import java.sql.*;

/**
 * Created by Iarina Dafin on 17/10/24
 */

public class Main {

    private Connection  connection;
    private Statement statement;

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
     * Example query made on the db.
     */
    private void query () {
        System.out.println("Test query of the first 10 lines of the CSV.");
        try {
            ResultSet result = statement.executeQuery("SELECT * FROM database LIMIT 10;");

            while (result.next()) {
                System.out.println(result.getInt("userID") + " " + result.getInt("itemID") + " " + result.getInt("rating"));
            }
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
        int size = computeSize();
        int count = 0;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM database LIMIT ? OFFSET 1;");
            ResultSet results;

            while (count <= size) {
                preparedStatement.setInt(1, count);
                results = preparedStatement.executeQuery();
                count++;

                // space for any computation
            }
            System.out.println("Database queried: " + count + " records.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Queries and returns the number of rows in a database.
     * @return the number of rows in the queried database
     */
    private int computeSize () {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM database");
            ResultSet result = preparedStatement.executeQuery();
            return result.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * @param args the command line arguments
     */
    public static void main (String[] args) throws SQLException {

        long startTime = System.currentTimeMillis();

        Main main = new Main();

        main.connectDatabase();
        //main.query();
        main.loadData();

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