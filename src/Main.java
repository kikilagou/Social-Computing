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
     * Creates a new database if needed.
     */
    public void createDatabase () {
        dropTable();

        try {
            DatabaseMetaData meta = connection.getMetaData();
            System.out.println("The driver name is " + meta.getDriverName());
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

    private void dropTable () {
        try {
            statement.execute("DROP TABLE IF EXISTS database;");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main (String[] args) throws SQLException {
        Main main = new Main();

        main.connectDatabase();

        main.query();

        try {
            main.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}