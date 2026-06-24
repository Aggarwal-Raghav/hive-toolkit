/*
 * Copyright 2025 Raghav Aggarwal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.raghav;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;

public class ConcurrentHMSTesting {

  private static final String DB_NAME = "test_db";
  private static final int TOTAL_TABLES = 10000;
  private static final int THREAD_COUNT = 50;
  private static final String HMS_URI = "thrift://localhost:9083";

  private static Configuration getConf() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, HMS_URI);
    return conf;
  }

  // ThreadLocal ensures every thread in our Executor gets a distinct, thread-safe HMS client
  private static final ThreadLocal<HiveMetaStoreClient> threadLocalClient =
      ThreadLocal.withInitial(
          () -> {
            try {
              return new HiveMetaStoreClient(getConf());
            } catch (Exception e) {
              throw new RuntimeException("Failed to create HiveMetaStoreClient", e);
            }
          });

  public static void main(String[] args) throws Exception {
    boolean dropDb = false;
    boolean skipCreate = false;

    for (String arg : args) {
      if (arg.equalsIgnoreCase("--drop")) {
        dropDb = true;
      } else if (arg.equalsIgnoreCase("--skip-create")) {
        skipCreate = true;
      }
    }

    HiveMetaStoreClient mainClient = new HiveMetaStoreClient(getConf());

    if (dropDb) {
      System.out.println("Dropping database '" + DB_NAME + "' if it exists...");
      try {
        mainClient.dropDatabase(DB_NAME, true, true, true);
        System.out.println("Database dropped.");
      } catch (Exception e) {
        System.out.println("Could not drop database: " + e.getMessage());
      }
    }

    // 1. Handle DB Creation
    System.out.println("Checking if database '" + DB_NAME + "' exists...");
    try {
      mainClient.getDatabase(DB_NAME);
      System.out.println("Database '" + DB_NAME + "' already exists.");
    } catch (NoSuchObjectException e) {
      System.out.println("Creating database '" + DB_NAME + "'...");
      Database db = new Database(DB_NAME, "Test database for HMS load", null, new HashMap<>());
      mainClient.createDatabase(db);
      System.out.println("Database created successfully.");
    }

    if (!skipCreate) {
      ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
      List<CompletableFuture<Void>> futures = new ArrayList<>();

      AtomicInteger successCount = new AtomicInteger(0);
      AtomicInteger failCount = new AtomicInteger(0);

      System.out.println(
          "Starting concurrent creation of "
              + TOTAL_TABLES
              + " tables using "
              + THREAD_COUNT
              + " threads...");
      long startCreate = System.currentTimeMillis();

      // 2. Concurrently Create Tables
      for (int i = 0; i < TOTAL_TABLES; i++) {
        final int tableIdx = i;
        CompletableFuture<Void> future =
            CompletableFuture.runAsync(
                () -> {
                  HiveMetaStoreClient client = threadLocalClient.get();
                  String tableName = "test_table_" + tableIdx;

                  try {
                    Table table = createTableObject(tableName);
                    client.createTable(table);

                    int currentSuccess = successCount.incrementAndGet();
                    // Print progress every 1000 successful creations
                    if (currentSuccess % 1000 == 0) {
                      System.out.println("Created " + currentSuccess + " tables so far...");
                    }
                  } catch (Exception ex) {
                    failCount.incrementAndGet();
                    System.err.println(
                        "Failed to create table " + tableName + ": " + ex.getMessage());
                  }
                },
                executor);

        futures.add(future);
      }

      // 3. Wait for creation to finish
      CompletableFuture<Void> allOf =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

      allOf.join(); // Block until all table creations are done

      long endCreate = System.currentTimeMillis();
      System.out.println("--------------------------------------------------");
      System.out.println("Table creation finished in " + (endCreate - startCreate) + " ms.");
      System.out.println("Success: " + successCount.get() + " | Failed: " + failCount.get());
      System.out.println("--------------------------------------------------");

      executor.shutdown();
    }

    // 4. Test getTables API
    System.out.println("Running get_tables() API (simulating 'show tables')...");
    long startGet = System.currentTimeMillis();

    try {
      // 1. Get the private thriftClient field from HiveMetaStoreClient
      List<String> tables = refGetTables(mainClient);

      long endGet = System.currentTimeMillis();
      System.out.println(
          "SUCCESS: Retrieved "
              + tables.size()
              + " table names via raw get_tables() in "
              + (endGet - startGet)
              + " ms.");

      System.out.println("Now running getTableObjectsByName (this triggers StackOverflow)...");
      List<Table> tableObjects = mainClient.getTableObjectsByName(DB_NAME, tables);
      System.out.println("Fetched " + tableObjects.size() + " full table objects.");

    } catch (Exception e) {
      System.err.println("API call failed with exception:");
      e.printStackTrace();
    }

    // 5. Cleanup
    mainClient.close();
    System.out.println("Shutdown complete.");
  }

  private static List<String> refGetTables(HiveMetaStoreClient mainClient)
      throws NoSuchFieldException, IllegalAccessException, TException {
    Field thriftClientField = mainClient.getClass().getDeclaredField("thriftClient");
    thriftClientField.setAccessible(true);
    Object thriftClientObj = thriftClientField.get(mainClient);

    // 2. Get the private client field from ThriftHiveMetaStoreClient
    Field clientField = thriftClientObj.getClass().getDeclaredField("client");
    clientField.setAccessible(true);

    // 3. Extract the raw Thrift Interface
    ThriftHiveMetastore.Iface rawThriftClient =
        (ThriftHiveMetastore.Iface) clientField.get(thriftClientObj);

    // 3. Directly invoke the raw get_tables API!
    return rawThriftClient.get_tables(DB_NAME, ".*");
  }

  /** Helper to build a minimal valid HMS Table object in Parquet format. */
  private static Table createTableObject(String tableName) {
    Table table = new Table();
    table.setDbName(ConcurrentHMSTesting.DB_NAME);
    table.setTableName(tableName);
    table.putToParameters("EXTERNAL", "TRUE");
    table.setTableType("EXTERNAL_TABLE");
    table.setOwner("hive");

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(
        Arrays.asList(
            new FieldSchema("id", "int", "identifier"),
            new FieldSchema("name", "string", "name field")));

    // Use Parquet format classes
    sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");

    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setName(tableName);
    serdeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
    sd.setSerdeInfo(serdeInfo);

    table.setSd(sd);
    return table;
  }
}
