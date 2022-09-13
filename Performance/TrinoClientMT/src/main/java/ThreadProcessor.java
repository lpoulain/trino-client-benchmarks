import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.zip.GZIPInputStream;

public class ThreadProcessor extends Thread
{
    TrinoClientMT client;
    TrinoClientMT.Results results;
    String url;

    public ThreadProcessor(TrinoClientMT client, TrinoClientMT.Results results, String url)
    {
        this.client = client;
        this.results = results;
        this.url = url;
    }

    @Override
    public void run() {
//            System.out.println("Processing new chunk: ");
        client.processData(results, this.url);
    }
}
