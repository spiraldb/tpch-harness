package dev.vortex.spark;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;


public final class Tpch {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("tpch_benchmark").getOrCreate();

        String format = "vortex";
        if (args.length >= 1) {
            format = args[0];
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

        for (int query = 1; query <= 22; query++) {
            timeQuery(sparkSession, query);
        }
    }

    private static void timeQuery(SparkSession spark, int query) {
        long startTime = System.nanoTime();
        String querySql = loadQuery(query);
        try {
            spark.sql(querySql).count();
        } finally {
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double elapsedSecs = ((double) duration) / 1_000_000_000.0;
            System.err.println("Query " + query + " took " + elapsedSecs + " seconds");
        }
    }

    private static String loadQuery(int query) {
        try (InputStream inputStream = Tpch.class.getClassLoader().getResourceAsStream("q" + query + ".sql")) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read query out of classpath", e);
        }
    }
}
