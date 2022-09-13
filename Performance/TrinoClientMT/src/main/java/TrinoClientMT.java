import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TrinoClientMT implements Runnable
{
    public class Results
    {
        Results next;
        Results prev;
        List<List<Object>> content;
        int index;
        Semaphore binarySemaphore;


        public Results(Results prev) {
            this.prev = prev;
            this.index = 0;
            this.binarySemaphore = new Semaphore(1);
            try {
                this.binarySemaphore.acquire();
            } catch (Exception e) { }
            this.content = Collections.emptyList();
        }

        public void unlock() {
            binarySemaphore.release();
        }

        public String next()
        {
            if (index >= content.size()) {
                return null;
            }
            return content.get(index++).get(5).toString();
        }
    }

    private String sql;
    private Results firstResults;
    private Results resultsCursor;
    LocalDateTime start;
    Semaphore binarySemaphore;

    public void processData(Results results, String nextUri) {
        String json = "";
        try {
            URL url = new URL(nextUri);
            URLConnection con = url.openConnection();
            HttpURLConnection http = (HttpURLConnection) con;
            http.setRequestMethod("GET");
//            GZIPInputStream gis = new GZIPInputStream(http.getInputStream());
            InputStream is = http.getInputStream();
            String headerField = http.getHeaderField("Content-Encoding");
            if (this.sql.equals("") || (headerField != null && headerField.equals("gzip"))) {
                json = new String(new GZIPInputStream(is).readAllBytes());
            } else {
                json = new String(is.readAllBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (json.length() > 600) {
            Any obj = JsonIterator.deserialize(json);

            results.content = new ArrayList<>();

            try {
                if (obj.keys().contains("data")) {
                    Any data = obj.get("data");
                    for (Any row : data.asList()) {
                        results.content.add(row.asList().stream().map(Any::object).collect(Collectors.toList()));
                    }
                }
            } catch (Exception e) {
            }
/*            try {
                ObjectMapper objectMapper = new ObjectMapper();
//                JsonNode rootNode = objectMapper.readTree(json);
                Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String,Object>>(){});

                if (jsonMap.containsKey("data")) {
                    results.content = (List<List<Object>>)jsonMap.get("data");
                }
            } catch (Exception e) { }*/
//            System.out.println("Response block processed");
        }

        if (firstResults == null) {
            firstResults = results;
            resultsCursor = results;

            binarySemaphore.release();
        } else {
            if (results.prev != null)
                results.prev.unlock();
        }
    }

    private void followTrail(String nextUri) throws Exception {
        Results prevResults = null;
        ExecutorService executorService = Executors.newFixedThreadPool(64);

        int counter = 0;
        while (nextUri != null && !nextUri.equals("")) {
            URL url = new URL(nextUri);
            URLConnection con = url.openConnection();
            HttpURLConnection http = (HttpURLConnection) con;
            http.setRequestMethod("HEAD");
//            GZIPInputStream gis = new GZIPInputStream(http.getInputStream());
            InputStream is = http.getInputStream();
//            byte[] response = gis.readNBytes(400);

//            LocalDateTime start = LocalDateTime.now();

//            String json = new String(response, StandardCharsets.UTF_8);

            Results results = new Results(prevResults);
            if (prevResults != null) {
                prevResults.next = results;
            }
//            ThreadProcessor tp = new ThreadProcessor(this, results, "", start, gis);
//            tp.start();
            String uri = nextUri;
            executorService.submit(() -> processData(results, uri));

            nextUri = http.getHeaderField("X-Trino-NextUri");
            counter++;
            prevResults = results;
        }

        if (prevResults != null) {
            prevResults.unlock();
        }

        LocalDateTime end = LocalDateTime.now();

        Duration duration = Duration.between(this.start, end);
        System.out.printf("Trail processed in %2d.%06ds (%d responses)\n", duration.getSeconds(), duration.getNano()/1000, counter);
//        executorService.awaitTermination(100, TimeUnit.DAYS);
        executorService.shutdown();
    }

    public void downloadAllUrls() throws Exception {
        Results prevResults = null;
        ExecutorService executorService = Executors.newFixedThreadPool(64);
        int counter = 0;
        List<String> urls = new ArrayList<>();

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    "urls.txt"));
            String line = reader.readLine();
            while (line != null) {
                if (!line.equals("")) {
                    urls.add(line);
                }
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        for (String nextUri : urls) {
            counter++;
            URL url = new URL(nextUri);
            URLConnection con = url.openConnection();
            HttpURLConnection http = (HttpURLConnection) con;
            http.setRequestMethod("HEAD");
//            Map<String, List<String>> headers = http.getHeaderFields();

            Results results = new Results(prevResults);
            if (prevResults != null) {
                prevResults.next = results;
            }
//            ThreadProcessor tp = new ThreadProcessor(this, results, "", start, gis);
//            tp.start();
            String uri = nextUri;
            executorService.submit(() -> processData(results, uri));
//            processData(results, uri);

            prevResults = results;
        }

        if (prevResults != null) {
            prevResults.unlock();
        }

        LocalDateTime end = LocalDateTime.now();

        Duration duration = Duration.between(this.start, end);
        System.out.printf("Trail processed in %2d.%06ds (%d responses)\n", duration.getSeconds(), duration.getNano()/1000, counter);
//        executorService.shutdown();
    }

    private void execute(String sql) throws Exception {
        if (sql == "") {
            downloadAllUrls();
        } else {
            URL url = new URL("http://localhost:8081/v1/statement");
            URLConnection con = url.openConnection();
            HttpURLConnection http = (HttpURLConnection) con;
            http.setRequestMethod("POST"); // PUT is another valid option
            http.setRequestProperty("X-Trino-User", "user");

            http.setDoOutput(true);

            OutputStream os = http.getOutputStream();
            os.write(sql.getBytes());

            byte[] response = http.getInputStream().readAllBytes();
            String json = new String(response, StandardCharsets.UTF_8);

            followTrail(getNextUri(json));
        }
    }

    private String getNextUri(String json) {
        int idxStart = json.indexOf("nextUri");
        if (idxStart < 0) {
            return null;
        }
        int idxEnd = json.indexOf('"', idxStart + 10);
        return json.substring(idxStart + 10, idxEnd);
    }

    public String nextResult() throws Exception {
        if (resultsCursor == null) {
//            System.out.println("Waiting for the first results");

            LocalDateTime start = LocalDateTime.now();
            binarySemaphore.acquire();
            LocalDateTime end = LocalDateTime.now();
            Duration duration = Duration.between(start, end);
//            System.out.printf("Results wait time: %2d.%06ds\n", duration.getSeconds(), duration.getNano()/1000);

//            System.out.println("Got the first results");
        }

        String result = resultsCursor.next();
        while (result == null) {
//            LocalDateTime start = LocalDateTime.now();
            resultsCursor.binarySemaphore.acquire();
//            LocalDateTime end = LocalDateTime.now();
//            Duration duration = Duration.between(start, end);
//            System.out.printf("Results wait time: %2d.%06ds\n", duration.getSeconds(), duration.getNano()/1000);
            resultsCursor = resultsCursor.next;
            if (resultsCursor == null) {
                return null;
            }
            result = resultsCursor.next();
        }

        return result;
    }

    public TrinoClientMT(String sql) {
        this.sql = sql;
        start = LocalDateTime.now();

        this.binarySemaphore = new Semaphore(1);
        try {
            this.binarySemaphore.acquire();
        } catch (Exception e) { }
    }

    @Override
    public void run() {
        try {
            execute(this.sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //////////////////////////////////////////////////////////////

    public static void executeQuery(String sql) throws Exception {
        Scanner myObj = new Scanner(System.in);
//        myObj.nextLine();

//        String sql = "SELECT * FROM tpch.sf100.orders LIMIT 10000000";
        TrinoClientMT client = new TrinoClientMT(sql);

        LocalDateTime start = LocalDateTime.now();
        Thread thread = new Thread(client);
        thread.setName("Trino trail follower");
        thread.start();

        int nb = 0, counter = 0;
        String result = client.nextResult();
        while (result != null) {
            nb++;
/*            if (nb % 10000 == 0) {
                System.out.println(nb);
            }*/
//            counter += result.length();
            result = client.nextResult();
        }

        LocalDateTime end = LocalDateTime.now();

        Duration duration = Duration.between(start, end);
        System.out.printf("[%s]: %2d.%06ds (%,d rows)\n", sql, duration.getSeconds(), duration.getNano()/1000, nb);

        myObj = new Scanner(System.in);
//        myObj.nextLine();

    }
/*
    public static void parallelDownoads() throws Exception {
        TrinoClientMT client = new TrinoClientMT("");

        LocalDateTime start = LocalDateTime.now();
        Thread thread = new Thread(client);
        thread.setName("Trino trail follower");
        thread.start();

        int nb = 0, counter = 0;
        String result = client.nextResult();
        while (result != null) {
            nb++;
//            counter += result.length();
            result = client.nextResult();
        }

        LocalDateTime end = LocalDateTime.now();

        Duration duration = Duration.between(start, end);
        System.out.printf("%2d.%06ds (%,d rows)\n", duration.getSeconds(), duration.getNano()/1000, nb);
    }
*/
    public static void main(String[] args) throws Exception
    {
//        executeQuery("SELECT * FROM tpch.sf100.orders LIMIT 10000000");
        executeQuery("");
    }
}
