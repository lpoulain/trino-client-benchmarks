import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.io.File;
import java.nio.file.Files;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.net.InetSocketAddress;
import java.io.OutputStream;

class TrinoMock implements HttpHandler {
    static List<Test> tests;
    static Map<String, String> initialQuery;
    static Map<String, Map<String, byte[]>> nextUri;

    public TrinoMock() throws Exception {
    }

    public static void init() throws Exception {
        tests = new ArrayList<Test>();
        initialQuery = new HashMap<>();
        nextUri = new HashMap<>();

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
                Test test = new Test(details[0], Integer.valueOf(details[1]), nbThreads);

                Path filePath;
                int testIdx = tests.size();
                String testIdxString = String.valueOf(tests.size());
                File folder = new File(String.format("./data/%d/", testIdx));
                File[] listOfFiles = folder.listFiles();
                tests.add(test);

                nextUri.put(testIdxString, new HashMap<>());
                
                if (listOfFiles == null) {
                    line = reader.readLine();
                    continue;
                }

                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        int nbBytes = 0;
                        if (file.getName().equals("query.json")) {
                            filePath = Path.of(String.format("./data/%d/query.json", testIdx));
                            String content = Files.readString(filePath);
                            nbBytes = content.length();
                            initialQuery.put(testIdxString, content);
                        } else {
                            filePath = Path.of(String.format("./data/%d/%s", testIdx, file.getName()));
                            byte[] data = Files.readAllBytes(filePath);
                            nbBytes = data.length;
                            String nextUriIdx = file.getName().split("\\.")[0];
                            Map<String, byte[]> nextUris = nextUri.get(testIdxString);
                            nextUris.put(nextUriIdx, data);
                        }
                        System.out.println(String.format("Read %s (%d bytes)", file.getName(), nbBytes));
                    }
                }

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {
            System.out.println(String.format("Error reading queries.txt: %s\n", e));
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            init();
            TrinoMock trinoServer = new TrinoMock();
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 8081), 0);
            HttpContext context = server.createContext("/", trinoServer);
//        context.setHandler(TrinoServer::handleRequest);
            server.start();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                System.out.println(ste);
            }
        }
    }

    private static void handleRequest(HttpExchange httpExchange) throws IOException {
        System.out.println(String.format("%s request for %s", httpExchange.getRequestMethod(), httpExchange.getRequestURI().getPath()));
        if (httpExchange.getRequestMethod().equals("POST")) {
            handleInitialQuery(httpExchange);
        } else {
            handleNextUri(httpExchange);
        }
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        System.out.println(String.format("%s request for %s", httpExchange.getRequestMethod(), httpExchange.getRequestURI().getPath()));
        if (httpExchange.getRequestMethod().equals("POST")) {
            handleInitialQuery(httpExchange);
        } else {
            handleNextUri(httpExchange);
        }
/*         OutputStream outputStream = httpExchange.getResponseBody();
        httpExchange.sendResponseHeaders(200, 0);
        outputStream.flush();
        outputStream.close();*/
    }

    private static void handleInitialQuery(HttpExchange httpExchange) throws IOException {
        String content = initialQuery.get("2");
        httpExchange.sendResponseHeaders(200, content.length());
        httpExchange.getRequestHeaders().set("Content-Type", "application/json");

//        httpExchange.getResponseBody().write(content.getBytes());
//        httpExchange.close();

        OutputStream outputStream = httpExchange.getResponseBody();
        outputStream.write(content.getBytes());
        outputStream.flush();
        outputStream.close();
    }

    private static void handleNextUri(HttpExchange httpExchange) throws IOException {
        byte[] data = nextUri.get("2").get("2");
        OutputStream outputStream = httpExchange.getResponseBody();
        httpExchange.sendResponseHeaders(200, data.length);
        httpExchange.getRequestHeaders().set("Content-Type", "application/json");
        httpExchange.getRequestHeaders().set("Content-Encoding", "gzip");
        outputStream.write(data);
//        outputStream.flush();
        outputStream.close();
    }

    public static class Test {
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
