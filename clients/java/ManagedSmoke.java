import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.net.ServerSocket;

/** Minimal opt-in managed lifecycle smoke test. */
public final class ManagedSmoke {
    public static void main(String[] args) throws Exception {
        if (args.length != 2 && args.length != 3) throw new IllegalArgumentException("data directory, executable, and optional tcp transport required");
        boolean tcp = args.length == 3 && "tcp".equals(args[2]);
        int port = 7379;
        if (tcp) try (ServerSocket reservation = new ServerSocket(0)) { port = reservation.getLocalPort(); }
        KuttiDBClient.ManagedServerOptions options = new KuttiDBClient.ManagedServerOptions(
                Path.of(args[0]), Path.of(args[1]), 250, 5000, null,
                tcp ? "tcp" : "unix", "127.0.0.1", port);
        try (KuttiDBClient client = KuttiDBClient.connectManaged(options)) {
            client.put("managed-java", "value".getBytes(StandardCharsets.UTF_8));
            byte[] value = client.get("managed-java");
            if (value == null || !"value".equals(new String(value, StandardCharsets.UTF_8)))
                throw new AssertionError("managed Java value mismatch");
        }
    }
}
