import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.LocalDateTime;

class PerfJDBC {
    Connection connection;

    private void run(String testName, String sql, int iterations) throws Exception {
        Statement stmt = connection.createStatement();

        LocalDateTime start = LocalDateTime.now();

	for (int i=0; i<iterations; i++) {
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) { }
        }

        LocalDateTime end = LocalDateTime.now();

        Duration duration = Duration.between(start, end);
        System.out.println(String.format("[%s] x %4d: %d.%06ds", sql, iterations, duration.getSeconds(), duration.getNano()/1000));
    }

    public PerfJDBC() throws Exception {
        String url = "jdbc:trino://localhost:8080/memory/information_schema?user=test";
        connection = DriverManager.getConnection(url);

        try {
            run("Small query", "SELECT 1                                    ", 1000);
            run("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 10);
        } catch (Exception e) {
            System.out.println("Error");
        }
    }

    public static void main(String[] args) throws Exception {
        new PerfJDBC();
    }
}

