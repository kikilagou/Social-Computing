import java.io.File;
import java.sql.*;

/**
 * Created by Iarina Dafin on 17/10/24.
 */

public class Main {

    /**
     * author: sqlitetutorial.net
     * found at: https://sqlite.org/quickstart.html
     *
     * Connects to a new database.
     * @param fileName the database file name
     */
    public static void createNewDatabase (String fileName) {

        String url = "jdbc:sqlite:" + System.getProperty("user.dir").replace('/', '\\') + fileName;

        try {
            Connection conn = DriverManager.getConnection(url);

            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

            String db = "jdbc:sqlite:SQLite";

            // SQL statement for creating a new table
            String sql = "CREATE TABLE test (\n"
                    + "	key integer PRIMARY KEY,\n"
                    + "	id int,\n"
                    + "	name varchar(255)"
                    + ");";

            Statement stmt = conn.createStatement();
            // create a new table
            stmt.execute(sql);

            String sqlinsert = "INSERT INTO test (id, name) VALUES(?,?)";

            PreparedStatement pstmt = conn.prepareStatement(sqlinsert);
            pstmt.setInt(1, 13);
            pstmt.setString(2, "name");

            pstmt.executeUpdate();

            String sqlselect = "SELECT * FROM test";
            ResultSet rs = stmt.executeQuery(sqlselect);

            while (rs.next()) {
                System.out.println(rs.getInt("id") + rs.getString("name"));
            }

            String sqldrop = "DROP TABLE IF EXISTS test";
            stmt.execute(sqldrop);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main (String[] args) {
        createNewDatabase ("test.db");
    }
}