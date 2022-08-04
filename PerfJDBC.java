import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.lang.Thread;

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

    private void runMultithread(String testName, String sql, int nbThreads) throws Exception {
        List<Thread> threads = new ArrayList<Thread>();
        for (int i=0; i<nbThreads; i++) {
            threads.add(new Thread(() -> {
                try {
                    Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) { }
                } catch (Exception e) {
                    System.out.println("Error");
                }
            }));
        }

        LocalDateTime start = LocalDateTime.now();
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread: threads) {
            thread.join();
        }

        LocalDateTime end = LocalDateTime.now();

        Duration duration = Duration.between(start, end);
        System.out.println(String.format("[%s] x %4d threads: %d.%06ds", sql, nbThreads, duration.getSeconds(), duration.getNano()/1000));
    }

    public PerfJDBC() throws Exception {
        String url = "jdbc:trino://localhost:8080/memory/information_schema?user=test";
        connection = DriverManager.getConnection(url);

        run("Small query", "SELECT 1                                    ", 1000);
        run("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 10);
        run("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 1);
        runMultithread("Large query", "SELECT * FROM tpch.sf100.orders LIMIT 100000", 10);
    }

    public static void main(String[] args) throws Exception {
        new PerfJDBC();
    }
}

