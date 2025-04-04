package dev.vortex.spark;

import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;


public final class Tpch {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("tpch_benchmark").getOrCreate();

        String format = "vortex";
        int startFrom = 1;
        int endAt = 22;
        if (args.length >= 1) {
            format = args[0];
        }
        if (args.length >= 2) {
            startFrom = Integer.parseInt(args[1]);
        }
        if (args.length >= 3) {
            endAt = Integer.parseInt(args[2]);
        }
        System.out.println("Setting format for tests to \"" + format + "\"");

        sparkSession.read().format("iceberg").load(format + ".customer").registerTempTable("customer");
        sparkSession.read().format("iceberg").load(format + ".lineitem").registerTempTable("lineitem");
        sparkSession.read().format("iceberg").load(format + ".nation").registerTempTable("nation");
        sparkSession.read().format("iceberg").load(format + ".orders").registerTempTable("orders");
        sparkSession.read().format("iceberg").load(format + ".partsupp").registerTempTable("partsupp");
        sparkSession.read().format("iceberg").load(format + ".part").registerTempTable("part");
        sparkSession.read().format("iceberg").load(format + ".region").registerTempTable("region");
        sparkSession.read().format("iceberg").load(format + ".supplier").registerTempTable("supplier");

        // warm the context just a bit.
        sparkSession.sql("select * from customer").count();
        sparkSession.sql("select * from lineitem").count();
        sparkSession.sql("select * from nation").count();
        sparkSession.sql("select * from orders").count();
        sparkSession.sql("select * from partsupp").count();
        sparkSession.sql("select * from part").count();
        sparkSession.sql("select * from region").count();
        sparkSession.sql("select * from supplier").count();

        for (int query = startFrom; query <= endAt; query++) {
            timeQuery(sparkSession, query);
        }
    }

    private static void timeQuery(SparkSession spark, int query) {
        long startTime = System.nanoTime();
        String querySql = loadQuery(query);
        try {
            if (query == 15) {
                String[] stmts = querySql.split(";");
                for (String stat : stmts) {
                    spark.sql(stat).collect();
                }
            } else {
                spark.sql(querySql).collect();
            }
        } finally {
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double elapsedSecs = ((double) duration) / 1_000_000_000.0;
            System.err.println("Query " + query + " took " + elapsedSecs + " seconds");
        }
    }

    private static String loadQuery(int query) {
        try (InputStream inputStream = Tpch.class.getResourceAsStream("q" + query + ".sql")) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read " + "q" + query + ".sql" + " out of classpath", e);
        }
    }
}
