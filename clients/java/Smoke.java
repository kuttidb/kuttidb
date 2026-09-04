import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Smoke {
    static KuttiDBClient connect() throws Exception {
        String token = System.getenv("KUTTIDB_AUTH_TOKEN");
        return token == null || token.isEmpty()
                ? new KuttiDBClient("127.0.0.1", 7394)
                : new KuttiDBClient("127.0.0.1", 7394, token.getBytes("UTF-8"));
    }

    public static void main(String[] args) throws Exception {
        try (KuttiDBClient c = connect()) {
            c.put("hello", "world".getBytes());
            assertEqual("world", new String(c.get("hello")));
            if (c.get("nope") != null) throw new AssertionError("expected miss");
            if (!c.delete("hello")) throw new AssertionError("delete should hit");

            c.put("ttl-key", "brief".getBytes(), 30000);
            assertEqual("brief", new String(c.get("ttl-key")));

            Map<String, byte[]> pairs = new HashMap<>();
            for (int i = 0; i < 5000; i++) pairs.put("j" + i, ("v" + i).getBytes());
            c.putMany(pairs);

            List<KuttiDBClient.Item> items = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                long ttl = (i % 2 == 0) ? 0 : 30_000;
                items.add(new KuttiDBClient.Item("jt" + i, "y10".getBytes(), ttl));
            }
            c.putManyTTL(items);
            assertEqual("y10", new String(c.get("jt0")));
            assertEqual("y10", new String(c.get("jt1")));

            ExecutorService ex = Executors.newFixedThreadPool(8);
            List<String> allKeys = new ArrayList<>(pairs.keySet());
            for (int w = 0; w < 8; w++) {
                final int id = w;
                ex.submit(() -> {
                    try (KuttiDBClient cc = connect()) {
                        List<String> keys = allKeys.subList(id * 625, (id + 1) * 625);
                        byte[][] got = cc.getMany(keys);
                        for (int i = 0; i < keys.size(); i++) {
                            String want = "v" + keys.get(i).substring(1);
                            assertEqual(want, new String(got[i]));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            ex.shutdown();
            ex.awaitTermination(60, TimeUnit.SECONDS);
            System.out.println("JAVA CLIENT OK — stats: " + c.stats());
        }
    }

    static void assertEqual(String a, String b) {
        if (!a.equals(b)) throw new AssertionError(a + " != " + b);
    }
}
