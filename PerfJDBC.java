import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.lang.Thread;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.net.InetSocketAddress;
import java.io.OutputStream;

class PerfJDBC implements HttpHandler {
    List<Test> tests;
    Connection connection0;
    Connection connection1;

    private void run(Connection connection, String testName, String sql, int iterations) throws Exception {
        Statement stmt = connection.createStatement();

        LocalDateTime start = LocalDateTime.now();

    	for (int i=0; i<iterations; i++) {
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) { }
        }

        LocalDateTime end = LocalDateTime.now();

        Duration duration = Duration.between(start, end);
        System.out.println(String.format("[%s] x %4d: %2d.%06ds", sql, iterations, duration.getSeconds(), duration.getNano()/1000));
    }

    private void runMultithread(Connection connection, String testName, String sql, int nbThreads) throws Exception {
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
        System.out.println(String.format("[%s] x %d threads: %d.%06ds", sql, nbThreads, duration.getSeconds(), duration.getNano()/1000));
    }

    public PerfJDBC() throws Exception {
        String url0 = "jdbc:trino://localhost:8080/memory/information_schema?user=test";
        connection0 = DriverManager.getConnection(url0);
        String url1 = "jdbc:trino://localhost:8081/memory/information_schema?user=test";
        connection1 = DriverManager.getConnection(url1);
        this.tests = new ArrayList<Test>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader("queries.txt"));
            String line = reader.readLine();
            while (line != null) {
                if (line == "") {
                    line = reader.readLine();
                    continue;
                }
                String[] details = line.split("\\|");

                if (details[0].isEmpty()) {
                    line = reader.readLine();
                    continue;
                }
                int nbThreads = Integer.valueOf(details[2]);
                tests.add(new Test(details[0], Integer.valueOf(details[1]), nbThreads));

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {
            System.out.println(String.format("Error reding queries.txt: %s\n", e));
        }
    }

    public void runTests(Boolean mock) throws Exception {
        Connection conn = mock ? connection1 : connection0;

        for (Test test : this.tests) {
            if (test.nbThreads > 1) {
                runMultithread(conn, "", test.sql, test.nbThreads);
            } else {
                run(conn, "", test.sql, test.nbIterations);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Boolean mock = false;
        Boolean runServer = false;

        for (String arg : args) {
            runServer |= (arg.equals("--server"));
            runServer |= (arg.equals("-server"));
            mock |= (arg.equals("--mock"));
            mock |= (arg.equals("-mock"));
        }

        if (runServer) {
            System.out.println("Python client starting at port 3003");
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 3003), 0);
            server.createContext("/", new PerfJDBC());
            server.start();
        } else {
            PerfJDBC perf = new PerfJDBC();
            perf.runTests(mock);
        }
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        try {
            Connection conn = connection0;
            String[] params = httpExchange.getRequestURI().getQuery().split("&");
            int testNb = 2;
            Boolean mock = false;
            for (String param : params) {
                if (param.startsWith("test=")) {
                    testNb = Integer.valueOf(param.split("=")[1]);
                } else if (param.contains("trino=mock")) {
                    conn = connection1;
                    mock = true;
                }
            }
            Test test = tests.get(testNb);

            System.out.println(String.format("Request: test %d (mock=%s)", testNb, mock));
            if (test.nbThreads > 1) {
                runMultithread(conn, "", test.sql, test.nbThreads);
            } else {
                run(conn, "", test.sql, test.nbIterations);
            }
        } catch (Exception e) {
            System.out.println(String.format("Error during request: %s", e.getMessage()));
            for (StackTraceElement ste : e.getStackTrace()) {
                System.out.println(ste);
            }
        }

        OutputStream outputStream = httpExchange.getResponseBody();
        httpExchange.sendResponseHeaders(200, 0);
        outputStream.flush();
        outputStream.close();
    }

    class Test {
        public String sql;
        public int nbIterations;
        public int nbThreads;

        public Test(String sql, int nbIterations, int nbThreads) {
            this.sql = sql;
            this.nbIterations = nbIterations;
            this.nbThreads = nbThreads;
        }
    }
}
