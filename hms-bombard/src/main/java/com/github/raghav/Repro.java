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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

public class Repro {
  private static final String DB_NAME = "test";
  private static final String LOCATION = "/warehouse/tablespace/external/hive/" + DB_NAME + ".db/";
  private static final String TABLE_PREFIX = "test_table";
  private static final String HOSTNAME = "localhost";
  private static final int THREAD_POOL_SIZE = 32;

  private static final ThreadLocal<HiveMetaStoreClient> CLIENT =
      ThreadLocal.withInitial(
          () -> {
            HiveConf conf = new HiveConf();
            conf.set("hive.metastore.uris", "thrift://" + HOSTNAME + ":9083");
            try {
              return new HiveMetaStoreClient(conf);
            } catch (MetaException e) {
              throw new RuntimeException(e);
            }
          });

  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

  private static <R> CompletableFuture<R> run(
      MyFunction<? super HiveMetaStoreClient, ? extends R> fn) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return fn.call(CLIENT.get());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        EXECUTOR);
  }

  public static void main(String[] args) throws Exception {
    try {
      if ("clear".equals(args[0])) {
        clearAll().get();
      } else {
        createDatabase(DB_NAME);
        int count = Integer.parseInt(args[0]);

        ArrayList<CompletableFuture<?>> cfs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
          CompletableFuture<?> cf =
              createTable(i)
                  .thenComposeAsync(stats -> createStats(stats).thenApply(x -> stats), EXECUTOR);
          // .thenComposeAsync(stats -> deleteTable(stats), EXECUTOR);
          cfs.add(cf);
        }

        CompletableFuture.allOf(cfs.toArray(new CompletableFuture<?>[0])).get();
      }
    } finally {
      EXECUTOR.shutdown();
      EXECUTOR.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static CompletableFuture<?> createStats(TableCreated stats) {
    ArrayList<CompletableFuture<?>> cfs = new ArrayList<>();
    for (ColumnStatistics colStat : stats.colStats) {
      CompletableFuture<Object> f =
          run(
              client -> {
                client.updateTableColumnStatistics(colStat);
                System.out.println("Updated col stats for table " + stats.tableName);
                return null;
              });
      cfs.add(f);
    }
    return CompletableFuture.allOf(cfs.toArray(new CompletableFuture<?>[0]));
  }

  private static void createDatabase(String dbName) throws TException {
    Database db = new Database(dbName, "java created database", LOCATION, null);
    HiveConf conf = new HiveConf();
    conf.set("hive.metastore.uris", "thrift://" + HOSTNAME + ":9083");
    HiveMetaStoreClient hms = new HiveMetaStoreClient(conf);
    hms.createDatabase(db);
  }

  private static CompletableFuture<TableCreated> createTable(int j) {
    String name = TABLE_PREFIX + "_" + j;

    return run(
        client -> {
          Table table = new Table();
          table.setDbName(DB_NAME);
          table.setTableName(name);
          table.setOwner("raghav");
          table.putToParameters("EXTERNAL", "TRUE");
          table.setTableType("EXTERNAL_TABLE");

          StorageDescriptor sd = new StorageDescriptor();
          sd.setLocation(LOCATION + name);
          sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
          sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");

          ArrayList<ColumnStatistics> colStatsList = new ArrayList<>();

          for (int i = 0; i < 10; i++) {
            String colName = "col_" + i;
            FieldSchema col1 = new FieldSchema(colName, "string", "");
            sd.addToCols(col1);

            ColumnStatisticsData colStatData = new ColumnStatisticsData();
            colStatData.setStringStats(new StringColumnStatsData(3, 3.0, 0, 1));

            ColumnStatisticsObj colStatsObj =
                new ColumnStatisticsObj(colName, "string", colStatData);
            List<ColumnStatisticsObj> colStatsObjs = java.util.Arrays.asList(colStatsObj);
            ColumnStatisticsDesc colStatsDesc = new ColumnStatisticsDesc(true, DB_NAME, name);

            ColumnStatistics colStats = new ColumnStatistics(colStatsDesc, colStatsObjs);
            colStats.setEngine("hive");
            colStatsList.add(colStats);

            colStats = new ColumnStatistics(colStatsDesc, colStatsObjs);
            colStats.setEngine("impala");
            colStatsList.add(colStats);
          }

          table.setSd(sd);
          sd.setSerdeInfo(new SerDeInfo());
          sd.getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");

          client.createTable(table);
          System.out.println("Created table " + name);

          return new TableCreated(colStatsList, table.getDbName(), table.getTableName());
        });
  }

  private static CompletableFuture<?> deleteTable(TableCreated t) {
    return run(
        client -> {
          client.dropTable(t.dbName, t.tableName);
          System.out.println("Dropped table " + t.tableName);
          return null;
        });
  }

  private static CompletableFuture<?> clearAll() {
    return run(
        client -> {
          List<String> tables = client.getTables(DB_NAME, TABLE_PREFIX + "*");
          for (String table : tables) {
            client.dropTable(DB_NAME, table);
            System.out.println("Dropped table " + table);
          }
          return null;
        });
  }

  private static class TableCreated {
    final List<? extends ColumnStatistics> colStats;
    final String dbName;
    final String tableName;

    public TableCreated(
        List<? extends ColumnStatistics> colStats, String dbName, String tableName) {
      this.colStats = colStats;
      this.dbName = dbName;
      this.tableName = tableName;
    }
  }

  @FunctionalInterface
  interface MyFunction<T, R> {
    R call(T t) throws Exception;
  }
}
