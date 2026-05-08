import com.aerospike.client.*;
import com.aerospike.client.policy.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class OracleToAerospike_10M_ROWID {

    private static final int THREAD_COUNT = 25;        // Good balance for 10M rows
    private static final int BATCH_SIZE = 5000;        // Your current batch size
    private static final int QUEUE_CAPACITY = 200000;  // Increased for 10M

    private final BlockingQueue<List<RowData>> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    // Oracle Config
    private final String oracleUrl = "jdbc:oracle:thin:@//host:port/service";
    private final String oracleUser = "user";
    private final String oraclePass = "pass";

    // Aerospike Config
    private final String aeroHost = "aerospike-host";
    private final int aeroPort = 3000;
    private final String namespace = "test";
    private final String setName = "target_set";

    private AerospikeClient aeroClient;

    public void start() throws Exception {
        ClientPolicy cp = new ClientPolicy();
        cp.maxConnsPerNode = 500;
        aeroClient = new AerospikeClient(cp, aeroHost, aeroPort);

        // Get proper ROWID ranges
        List<RowidRange> ranges = getRowidRanges(THREAD_COUNT);

        System.out.println("Starting ETL for ~10M rows with " + ranges.size() + " parallel ranges.");

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < ranges.size(); i++) {
            final RowidRange range = ranges.get(i);
            final int threadId = i;
            futures.add(executor.submit(() -> readerTask(threadId, range)));
        }

        Thread writerThread = new Thread(this::writerTask);
        writerThread.start();

        for (Future<?> f : futures) f.get();

        queue.put(Collections.emptyList()); // Poison pill
        writerThread.join();

        executor.shutdown();
        aeroClient.close();
        System.out.println("=== ETL Completed Successfully ===");
    }

    // ==================== PROPER ROWID RANGE CALCULATION ====================
    private List<RowidRange> getRowidRanges(int numThreads) {
        List<RowidRange> ranges = new ArrayList<>();
        String sql = """
            SELECT MIN(rowid) min_rid, 
                   MAX(rowid) max_rid, 
                   COUNT(*) total_rows 
            FROM source_table
            """;

        try (Connection conn = DriverManager.getConnection(oracleUrl, oracleUser, oraclePass);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            if (rs.next()) {
                String minRid = rs.getString("min_rid");
                String maxRid = rs.getString("max_rid");
                long totalRows = rs.getLong("total_rows");

                System.out.println("Total rows in table: " + totalRows);

                // Get all ROWIDs in ordered fashion and split them
                String splitSql = """
                    SELECT rowid 
                    FROM (
                        SELECT rowid, 
                               NTILE(?) OVER (ORDER BY rowid) AS bucket
                        FROM source_table
                    ) 
                    WHERE bucket = 1
                    ORDER BY rowid
                    """;

                try (PreparedStatement psSplit = conn.prepareStatement(splitSql)) {
                    psSplit.setInt(1, numThreads);

                    List<String> splitPoints = new ArrayList<>();
                    splitPoints.add(minRid); // First range starts from min

                    try (ResultSet rsSplit = psSplit.executeQuery()) {
                        while (rsSplit.next()) {
                            splitPoints.add(rsSplit.getString("rowid"));
                        }
                    }

                    // Create ranges
                    for (int i = 0; i < numThreads; i++) {
                        String start = splitPoints.get(i);
                        String end = (i + 1 < splitPoints.size()) ? splitPoints.get(i + 1) : maxRid;
                        ranges.add(new RowidRange(start, end));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            // Fallback: single range
            ranges.add(new RowidRange(null, null));
        }
        return ranges;
    }

    private void readerTask(int threadId, RowidRange range) {
        String sql = """
            SELECT id_column, col1, col2, col3 /* ADD ALL COLUMNS YOU NEED */
            FROM source_table 
            WHERE rowid BETWEEN ? AND ?
            /*+ PARALLEL(6) */
            """;

        long count = 0;
        try (Connection conn = DriverManager.getConnection(oracleUrl, oracleUser, oraclePass);
             PreparedStatement ps = conn.prepareStatement(sql, 
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            ps.setFetchSize(15000);        // Higher for 10M scale
            ps.setString(1, range.startRowid);
            ps.setString(2, range.endRowid);

            try (ResultSet rs = ps.executeQuery()) {
                List<RowData> batch = new ArrayList<>(BATCH_SIZE);

                while (rs.next()) {
                    RowData row = new RowData();
                    row.keyValue = rs.getString("id_column");
                    row.col1 = rs.getObject("col1");
                    row.col2 = rs.getObject("col2");
                    row.col3 = rs.getObject("col3");
                    // Add other columns here...

                    batch.add(row);
                    count++;

                    if (batch.size() >= BATCH_SIZE) {
                        queue.put(new ArrayList<>(batch));
                        batch.clear();
                    }
                }

                if (!batch.isEmpty()) {
                    queue.put(new ArrayList<>(batch));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Thread-" + threadId + " completed → Rows processed: " + count);
    }

    private void writerTask() {
        WritePolicy wp = new WritePolicy();
        wp.recordExistsAction = RecordExistsAction.UPDATE;
        wp.socketTimeout = 10000;
        wp.totalTimeout = 20000;
        wp.maxRetries = 2;

        long totalWritten = 0;

        try {
            while (true) {
                List<RowData> batch = queue.take();
                if (batch.isEmpty()) break;

                List<Key> keys = new ArrayList<>(batch.size());
                List<Bin[]> binsList = new ArrayList<>(batch.size());

                for (RowData row : batch) {
                    Key key = new Key(namespace, setName, row.keyValue.toString());
                    keys.add(key);

                    binsList.add(new Bin[]{
                        new Bin("col1", row.col1),
                        new Bin("col2", row.col2),
                        new Bin("col3", row.col3)
                        // Add remaining bins...
                    });
                }

                aeroClient.operate(wp, new BatchWritePolicy(), 
                                  keys.toArray(new Key[0]), binsList.toArray(new Bin[0][]));

                totalWritten += batch.size();
                if (totalWritten % 100000 == 0) {
                    System.out.println("✅ Aerospike Written: " + totalWritten + " | Queue: " + queue.size());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class RowidRange {
        String startRowid;
        String endRowid;

        RowidRange(String start, String end) {
            this.startRowid = start;
            this.endRowid = end;
        }
    }

    static class RowData {
        Object keyValue;
        Object col1, col2, col3;
        // Add more fields as needed
    }

    public static void main(String[] args) throws Exception {
        new OracleToAerospike_10M_ROWID().start();
    }
}
