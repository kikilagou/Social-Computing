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
     * Connects to a new database.
     */
    private void createDatabase () {

        String url = "jdbc:sqlite:" + System.getProperty("user.dir").replace('/', '\\') + "\\SQLite\\" + "database.db";

        try {
            connection = DriverManager.getConnection(url);
            statement = connection.createStatement();

            DatabaseMetaData meta = connection.getMetaData();
            System.out.println("The driver name is " + meta.getDriverName());
            System.out.println("A new database has been created.");

            // SQL statement for creating a new table
            String sql = "CREATE TABLE database (\n"
                    + "	key integer PRIMARY KEY,\n"
                    + "	id int,\n"
                    + "	name varchar(255)"
                    + ");";

            statement.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private void insertInto () {
        try {
            PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO database (id, name) VALUES(?,?);");

            insertStatement.setInt(1, 13);
            insertStatement.setString(2, "name");

            insertStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private void query () {
        try {
            ResultSet result = statement.executeQuery("SELECT * FROM database;");

            while (result.next()) {
                System.out.println(result.getInt("id") + " " + result.getString("name"));
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
    public static void main (String[] args) {
        Main main = new Main();

        main.createDatabase();
        main.insertInto();
        main.query();

        try {
            main.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}