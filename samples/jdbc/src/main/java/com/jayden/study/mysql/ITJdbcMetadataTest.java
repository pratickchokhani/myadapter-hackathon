// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.jayden.study.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.common.collect.ImmutableList;
import com.jayden.study.utils.JdbcUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

public class ITJdbcMetadataTest {

  private static final String url = "jdbc:mysql://localhost:3306/test?autoReconnect=true&useSSL=false";
  private static final String user = "pratick";
  private static final String password = "password";

  private static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

  private static Connection connection;
  private static Random random = new Random();
  private static final String[] VERSIONS =
      new String[]{
          "8.0.30"
      };


  @BeforeClass
  public static void setup() throws Exception {
    connection = JdbcUtils.getConnection(url, user, password);
  }

  private void runForAllVersions(BiConsumer<Connection, String> runnable) throws Exception {
    for (String version : VERSIONS) {
      String mavenUrl =
          String.format(
              "https://repo1.maven.org/maven2/mysql/mysql-connector-java/%s/mysql-connector-java-%s.jar",
              version, version);
      // Create a class loader that will search the PostgreSQL JDBC driver jar that we download from
      // Maven and this class file for classes. In addition, we set the parent class loader of this
      // new class loader to be the same as the parent of the default class loader. This is the
      // class loader that will be used to load system classes. We do this because:
      // 1. We want to use a specific JDBC driver version that we download from Maven.
      // 2. We want to have access to the classes in this file.
      // 3. We do not want to have access to any of the other classes that are defined in this
      // repository (specifically: we do not want to have access to the PostgreSQL driver that is
      // included in the repository as a dependency). This is why we set the parent class loader to
      // be the system class loader (i.e. the parent of the default class loader), and we do not use
      // the default class loader as the parent for our custom class loader.
      System.out.println(mavenUrl);
      ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        ClassLoader classLoader =
            new URLClassLoader(
                new URL[]{
                    new URL(mavenUrl),
                    ITJdbcMetadataTest.class.getProtectionDomain().getCodeSource().getLocation()
                },
                defaultClassLoader.getParent());
        Thread.currentThread().setContextClassLoader(classLoader);
        // Verify that we can load the PG JDBC driver from this class loader.
        try {
          classLoader.loadClass(MYSQL_JDBC_DRIVER);
        } catch (Throwable t) {
          throw new Exception(
              "Could not load PostgreSQL driver using URLClassLoader: " + t.getMessage(), t);
        }
        Class<?> runnerClass = classLoader.loadClass(TestRunner.class.getName());
        Constructor<?> constructor = runnerClass.getDeclaredConstructor();
        Object runner = constructor.newInstance();
        Method runMethod =
            runner
                .getClass()
                .getDeclaredMethod("run", String.class, BiConsumer.class);
        runMethod.invoke(runner, version, runnable);
      } catch (Throwable t) {
        throw new Exception(String.format("Error for version %s:", version) + t.getMessage(), t);
      } finally {
        Thread.currentThread().setContextClassLoader(defaultClassLoader);
      }
    }
  }

  public static class TestRunner {

    public void run(String version, BiConsumer<Connection, String> runnable)
        throws Exception {
      // Dynamically load the PG JDBC driver using the default class loader of the current thread.
      Class.forName(MYSQL_JDBC_DRIVER);
      try (Connection connection = JdbcUtils.getConnection(url, user, password)) {
        runnable.accept(connection, version);
      }
      // Deregister the current driver from the DriverManager to prevent 'the driver has already
      // been registered' errors.
      Driver driver = DriverManager.getDriver(url);
      DriverManager.deregisterDriver(driver);
    }
  }

  @Test
  public void testDatabaseMetaDataVersion() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          int lastPointIndex = version.lastIndexOf('.');
          int patchVersion = Integer.parseInt(version.substring(lastPointIndex + 1));
          String comparableVersion =
              version.substring(0, lastPointIndex + 1) + String.format("%03d", patchVersion);
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // Options in the connection string are only supported from version 42.2.6.
            System.out.println(metadata.getDatabaseProductVersion());
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTables() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables =
                metadata.getTables(null, "test", null, new String[]{"TABLE"})) {
              while (tables.next()) {
                System.out.println(tables.getString("TABLE_NAME"));
              }
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTables_Unfiltered() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables = metadata.getTables(null, null, null, null)) {
              // Just assert that there is at least one result.
              assertTrue(tables.next());
              //noinspection StatementWithEmptyBody
              while (tables.next()) {
              }
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTables_FilteredByName() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables =
                metadata.getTables(null, "public", "%a%", new String[]{"TABLE"})) {
              assertTrue(tables.next());
              assertEquals("albums", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("all_types", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("recording_attempt", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("tracks", tables.getString("TABLE_NAME"));

              assertFalse(tables.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTables_TablesAndViews() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables =
                metadata.getTables(null, "public", "%a%", new String[]{"TABLE", "VIEW"})) {
              assertTrue(tables.next());
              assertEquals("albums", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("all_types", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("recording_attempt", tables.getString("TABLE_NAME"));
              assertTrue(tables.next());
              assertEquals("tracks", tables.getString("TABLE_NAME"));

            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTables_AllTypes() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables =
                metadata.getTables(
                    null,
                    "public",
                    "%a%",
                    new String[]{
                        "TABLE",
                        "PARTITIONED TABLE",
                        "INDEX",
                        "PARTITIONED INDEX",
                        "SEQUENCE",
                        "TYPE",
                        "SYSTEM TABLE",
                        "SYSTEM TOAST TABLE",
                        "SYSTEM TOAST INDEX",
                        "SYSTEM VIEW",
                        "SYSTEM INDEX",
                        "TEMPORARY TABLE",
                        "TEMPORARY INDEX",
                        "TEMPORARY VIEW",
                        "TEMPORARY SEQUENCE",
                        "FOREIGN TABLE",
                        "MATERIALIZED VIEW",
                        "VIEW"
                    })) {

              while (tables.next()) {
                System.out.println(tables.getString("TABLE_NAME"));
              }
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataColumns() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet columns = metadata.getColumns(null, "public", "all_types", null)) {
              assertTrue(columns.next());
              assertEquals("col_bigint", columns.getString("COLUMN_NAME"));
              assertEquals(Types.BIGINT, columns.getInt("DATA_TYPE"));
              assertEquals("BIGINT", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_bool", columns.getString("COLUMN_NAME"));
              assertEquals(Types.BIT, columns.getInt("DATA_TYPE"));
              assertEquals("BIT", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_bytea", columns.getString("COLUMN_NAME"));
              assertEquals(Types.LONGVARBINARY, columns.getInt("DATA_TYPE"));
              assertEquals("LONGBLOB", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_float8", columns.getString("COLUMN_NAME"));
              assertEquals(Types.DOUBLE, columns.getInt("DATA_TYPE"));
              assertEquals("DOUBLE", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_int", columns.getString("COLUMN_NAME"));
              assertEquals(Types.INTEGER, columns.getInt("DATA_TYPE"));
              assertEquals("INT", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_numeric", columns.getString("COLUMN_NAME"));
              assertEquals(Types.DECIMAL, columns.getInt("DATA_TYPE"));
              assertEquals("DECIMAL", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_timestamptz", columns.getString("COLUMN_NAME"));
              assertEquals(Types.TIMESTAMP, columns.getInt("DATA_TYPE"));
              String name = columns.getString("TYPE_NAME");
              assertTrue("TIMESTAMP".equals(name));

              assertTrue(columns.next());
              assertEquals("col_date", columns.getString("COLUMN_NAME"));
              assertEquals(Types.DATE, columns.getInt("DATA_TYPE"));
              assertEquals("DATE", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_varchar", columns.getString("COLUMN_NAME"));
              assertEquals(Types.VARCHAR, columns.getInt("DATA_TYPE"));
              assertEquals("VARCHAR", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_jsonb", columns.getString("COLUMN_NAME"));
              assertEquals(Types.LONGVARCHAR, columns.getInt("DATA_TYPE"));
              assertEquals("JSON", columns.getString("TYPE_NAME"));

              assertFalse(columns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataColumns_FilteredByName() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet columns = metadata.getColumns(null, "public", "all_types", "%a%")) {
              assertTrue(columns.next());
              assertEquals("col_bytea", columns.getString("COLUMN_NAME"));
              assertEquals(Types.LONGVARBINARY, columns.getInt("DATA_TYPE"));
              assertEquals("LONGBLOB", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_float8", columns.getString("COLUMN_NAME"));
              assertEquals(Types.DOUBLE, columns.getInt("DATA_TYPE"));
              assertEquals("DOUBLE", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_timestamptz", columns.getString("COLUMN_NAME"));
              assertEquals(Types.TIMESTAMP, columns.getInt("DATA_TYPE"));
              String name = columns.getString("TYPE_NAME");
              assertTrue("TIMESTAMP".equals(name));

              assertTrue(columns.next());
              assertEquals("col_date", columns.getString("COLUMN_NAME"));
              assertEquals(Types.DATE, columns.getInt("DATA_TYPE"));
              assertEquals("DATE", columns.getString("TYPE_NAME"));

              assertTrue(columns.next());
              assertEquals("col_varchar", columns.getString("COLUMN_NAME"));
              assertEquals(Types.VARCHAR, columns.getInt("DATA_TYPE"));
              assertEquals("VARCHAR", columns.getString("TYPE_NAME"));

              assertFalse(columns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataIndexInfo() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet indexInfo =
                metadata.getIndexInfo(null, "public", "all_types", false, false)) {
              assertTrue(indexInfo.next());
              assertEquals("col_bigint", indexInfo.getString("COLUMN_NAME"));
              assertEquals("PRIMARY", indexInfo.getString("INDEX_NAME"));
              assertTrue(indexInfo.next());
              assertEquals("col_varchar", indexInfo.getString("COLUMN_NAME"));
              assertEquals("idx_col_varchar_int", indexInfo.getString("INDEX_NAME"));
              assertTrue(indexInfo.next());
              assertEquals("col_int", indexInfo.getString("COLUMN_NAME"));
              assertEquals("idx_col_varchar_int", indexInfo.getString("INDEX_NAME"));

              assertFalse(indexInfo.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataIndexInfo_Unique() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet indexInfo =
                metadata.getIndexInfo(null, "public", "numbers", true, false)) {
              assertTrue(indexInfo.next());
              assertEquals("num", indexInfo.getString("COLUMN_NAME"));
              assertEquals("PRIMARY", indexInfo.getString("INDEX_NAME"));

              assertFalse(indexInfo.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataPrimaryKeys() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet primaryKeys = metadata.getPrimaryKeys(null, "test", "all_types")) {
              assertTrue(primaryKeys.next());
              assertEquals("col_bigint", primaryKeys.getString("COLUMN_NAME"));
              assertEquals(1, primaryKeys.getShort("KEY_SEQ"));
              assertFalse(primaryKeys.next());
            }
            try (ResultSet primaryKeys = metadata.getPrimaryKeys(null, "public", "tracks")) {
              assertTrue(primaryKeys.next());
              assertEquals("album_id", primaryKeys.getString("COLUMN_NAME"));
              assertEquals(1, primaryKeys.getShort("KEY_SEQ"));
              assertTrue(primaryKeys.next());
              assertEquals("track_number", primaryKeys.getString("COLUMN_NAME"));
              assertEquals(2, primaryKeys.getShort("KEY_SEQ"));
              assertFalse(primaryKeys.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataExportedKeys() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet exportedKeys = metadata.getExportedKeys(null, "test", "singers")) {
              // assertTrue(exportedKeys.next());
              // assertEquals("singers", exportedKeys.getString("PKTABLE_NAME"));
              // assertEquals("singer_id", exportedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("albums", exportedKeys.getString("FKTABLE_NAME"));
              // assertEquals("singer_id", exportedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(1, exportedKeys.getShort("KEY_SEQ"));
              // assertFalse(exportedKeys.next());
            }
            try (ResultSet exportedKeys = metadata.getExportedKeys(null, "public", "albums")) {
              // assertTrue(exportedKeys.next());
              // assertEquals("albums", exportedKeys.getString("PKTABLE_NAME"));
              // assertEquals("album_id", exportedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", exportedKeys.getString("FKTABLE_NAME"));
              // assertEquals("album", exportedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(1, exportedKeys.getShort("KEY_SEQ"));
              //
              // assertTrue(exportedKeys.next());
              // assertEquals("albums", exportedKeys.getString("PKTABLE_NAME"));
              // assertEquals("album_id", exportedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("tracks", exportedKeys.getString("FKTABLE_NAME"));
              // assertEquals("album_id", exportedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(1, exportedKeys.getShort("KEY_SEQ"));
              // assertFalse(exportedKeys.next());
            }
            try (ResultSet exportedKeys = metadata.getExportedKeys(null, "public", "tracks")) {
              // assertTrue(exportedKeys.next());
              // assertEquals("tracks", exportedKeys.getString("PKTABLE_NAME"));
              // assertEquals("album_id", exportedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", exportedKeys.getString("FKTABLE_NAME"));
              // assertEquals("album", exportedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(1, exportedKeys.getShort("KEY_SEQ"));
              // assertTrue(exportedKeys.next());
              // assertEquals("tracks", exportedKeys.getString("PKTABLE_NAME"));
              // assertEquals("track_number", exportedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", exportedKeys.getString("FKTABLE_NAME"));
              // assertEquals("track", exportedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(2, exportedKeys.getShort("KEY_SEQ"));
              //
              // assertFalse(exportedKeys.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataImportedKeys() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet importedKeys = metadata.getImportedKeys(null, "public", "albums")) {
              // assertTrue(importedKeys.next());
              // assertEquals("singers", importedKeys.getString("PKTABLE_NAME"));
              // assertEquals("singer_id", importedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("albums", importedKeys.getString("FKTABLE_NAME"));
              // assertEquals("singer_id", importedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(1, importedKeys.getShort("KEY_SEQ"));
              // assertFalse(importedKeys.next());
            }
            try (ResultSet importedKeys = metadata.getImportedKeys(null, "public", "tracks")) {
              // assertTrue(importedKeys.next());
              // assertEquals("albums", importedKeys.getString("PKTABLE_NAME"));
              // assertEquals("album_id", importedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("tracks", importedKeys.getString("FKTABLE_NAME"));
              // assertEquals("album_id", importedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(1, importedKeys.getShort("KEY_SEQ"));
              // assertFalse(importedKeys.next());
            }
            try (ResultSet importedKeys =
                metadata.getImportedKeys(null, "public", "recording_attempt")) {
              // assertTrue(importedKeys.next());
              // assertEquals("albums", importedKeys.getString("PKTABLE_NAME"));
              // assertEquals("album_id", importedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", importedKeys.getString("FKTABLE_NAME"));
              // assertEquals("album", importedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(1, importedKeys.getShort("KEY_SEQ"));
              //
              // assertTrue(importedKeys.next());
              // assertEquals("tracks", importedKeys.getString("PKTABLE_NAME"));
              // assertEquals("album_id", importedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", importedKeys.getString("FKTABLE_NAME"));
              // assertEquals("album", importedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(1, importedKeys.getShort("KEY_SEQ"));
              // assertTrue(importedKeys.next());
              // assertEquals("tracks", importedKeys.getString("PKTABLE_NAME"));
              // assertEquals("track_number", importedKeys.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", importedKeys.getString("FKTABLE_NAME"));
              // assertEquals("track", importedKeys.getString("FKCOLUMN_NAME"));
              // assertEquals(2, importedKeys.getShort("KEY_SEQ"));
              //
              // assertFalse(importedKeys.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataCrossReference() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // Retrieve the references from albums (foreign table) to singers (parent table).
            try (ResultSet references =
                metadata.getCrossReference(null, "public", "singers", null, "public", "albums")) {
              // assertTrue(references.next());
              // assertEquals("singers", references.getString("PKTABLE_NAME"));
              // assertEquals("singer_id", references.getString("PKCOLUMN_NAME"));
              // assertEquals("albums", references.getString("FKTABLE_NAME"));
              // assertEquals("singer_id", references.getString("FKCOLUMN_NAME"));
              // assertEquals(1, references.getShort("KEY_SEQ"));
              // assertFalse(references.next());
            }
            // Retrieve the references from tracks to albums.
            try (ResultSet references =
                metadata.getCrossReference(null, "public", "albums", null, "public", "tracks")) {
              // assertTrue(references.next());
              // assertEquals("albums", references.getString("PKTABLE_NAME"));
              // assertEquals("album_id", references.getString("PKCOLUMN_NAME"));
              // assertEquals("tracks", references.getString("FKTABLE_NAME"));
              // assertEquals("album_id", references.getString("FKCOLUMN_NAME"));
              // assertEquals(1, references.getShort("KEY_SEQ"));
              // assertFalse(references.next());
            }
            // Retrieve the references from recording_attempt to albums.
            try (ResultSet references =
                metadata.getCrossReference(
                    null, "public", "albums", null, "public", "recording_attempt")) {
              // assertTrue(references.next());
              // assertEquals("albums", references.getString("PKTABLE_NAME"));
              // assertEquals("album_id", references.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", references.getString("FKTABLE_NAME"));
              // assertEquals("album", references.getString("FKCOLUMN_NAME"));
              // assertEquals(1, references.getShort("KEY_SEQ"));
              //
              // assertFalse(references.next());
            }
            // Retrieve the references from recording_attempt to tracks.
            try (ResultSet references =
                metadata.getCrossReference(
                    null, "public", "tracks", null, "public", "recording_attempt")) {
              // assertTrue(references.next());
              // assertEquals("tracks", references.getString("PKTABLE_NAME"));
              // assertEquals("album_id", references.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", references.getString("FKTABLE_NAME"));
              // assertEquals("album", references.getString("FKCOLUMN_NAME"));
              // assertEquals(1, references.getShort("KEY_SEQ"));
              // assertTrue(references.next());
              // assertEquals("tracks", references.getString("PKTABLE_NAME"));
              // assertEquals("track_number", references.getString("PKCOLUMN_NAME"));
              // assertEquals("recording_attempt", references.getString("FKTABLE_NAME"));
              // assertEquals("track", references.getString("FKCOLUMN_NAME"));
              // assertEquals(2, references.getShort("KEY_SEQ"));
              //
              // assertFalse(references.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTypeInfo() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            Map<String, Integer> results = new HashMap<>();
            try (ResultSet types = metadata.getTypeInfo()) {
              while (types.next()) {
                results.put(types.getString("TYPE_NAME"), types.getInt("DATA_TYPE"));
              }
            }
            // assertTrue(results.containsKey("BIT"));
            // assertEquals(Types.BIT, results.get("BIT").intValue());
            // assertTrue(results.containsKey("LONGBLOB"));
            // assertEquals(Types.LONGVARBINARY, results.get("bytea").intValue());
            // assertTrue(results.containsKey("float8"));
            // assertEquals(Types.DOUBLE, results.get("float8").intValue());
            // assertTrue(results.containsKey("int8"));
            // assertEquals(Types.BIGINT, results.get("int8").intValue());
            // assertTrue(results.containsKey("numeric"));
            // assertEquals(Types.NUMERIC, results.get("numeric").intValue());
            // assertTrue(results.containsKey("timestamptz"));
            // assertEquals(Types.TIMESTAMP, results.get("timestamptz").intValue());
            // assertTrue(results.containsKey("date"));
            // assertEquals(Types.DATE, results.get("date").intValue());
            // assertTrue(results.containsKey("varchar"));
            // assertEquals(Types.VARCHAR, results.get("varchar").intValue());
            // assertTrue(results.containsKey("jsonb"));
            // assertEquals(Types.OTHER, results.get("jsonb").intValue());
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataSuperTypes() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // This method is not supported by the PG JDBC driver.
            metadata.getSuperTypes(null, null, null);
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTableTypes() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            boolean hasTable = false;
            boolean hasView = false;
            try (ResultSet tableTypes = metadata.getTableTypes()) {
              while (tableTypes.next()) {
                hasTable = hasTable || "TABLE".equals(tableTypes.getString("TABLE_TYPE"));
                hasView = hasView || "VIEW".equals(tableTypes.getString("TABLE_TYPE"));
              }
            }
            assertTrue(hasTable);
            assertTrue(hasView);
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataTablePrivileges() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tablePrivileges = metadata.getTablePrivileges(null, null, null)) {
              assertTrue(tablePrivileges.next());
            }
            try (ResultSet tablePrivileges =
                metadata.getTablePrivileges(null, "public", "all_types")) {
              assertFalse(tablePrivileges.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  @Ignore
  public void testDatabaseMetaDataColumnPrivileges() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // try (ResultSet columnPrivileges =
            //     metadata.getColumnPrivileges(null, null, null, null)) {
            //   assertFalse(columnPrivileges.next());
            // }
            // try (ResultSet columnPrivileges =
            //     metadata.getColumnPrivileges(null, "test", "all_types", null)) {
            //   assertFalse(columnPrivileges.next());
            // }
            try (ResultSet columnPrivileges =
                metadata.getColumnPrivileges(null,  "test", "all_types", "col_bigint")) {
              assertFalse(columnPrivileges.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataAttributes() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // This method is not supported by the PG JDBC driver.

            metadata.getAttributes(null, null, null, null);
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Ignore("Not yet supported")
  @Test
  public void testDatabaseMetaDataBestRowIdentifier() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // PG just returns the primary key of each table for this method.
            try (ResultSet rowIdentifiers =
                metadata.getBestRowIdentifier(
                    null, "public", "singers", DatabaseMetaData.bestRowTransaction, false)) {
              assertTrue(rowIdentifiers.next());
              assertEquals("singer_id", rowIdentifiers.getString("COLUMN_NAME"));
              assertEquals("int8", rowIdentifiers.getString("TYPE_NAME"));
              assertFalse(rowIdentifiers.next());
            }
            try (ResultSet rowIdentifiers =
                metadata.getBestRowIdentifier(
                    null, "public", "tracks", DatabaseMetaData.bestRowTransaction, false)) {
              assertTrue(rowIdentifiers.next());
              assertEquals("album_id", rowIdentifiers.getString("COLUMN_NAME"));
              assertEquals("int8", rowIdentifiers.getString("TYPE_NAME"));
              assertTrue(rowIdentifiers.next());
              assertEquals("track_number", rowIdentifiers.getString("COLUMN_NAME"));
              assertEquals("int8", rowIdentifiers.getString("TYPE_NAME"));
              assertFalse(rowIdentifiers.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataBestRowIdentifier_NoData() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            // The current replacement returns an empty result set.
            try (ResultSet rowIdentifiers =
                metadata.getBestRowIdentifier(
                    null, "public", "singers", DatabaseMetaData.bestRowTransaction, false)) {
              assertTrue(rowIdentifiers.next());
            }
            try (ResultSet rowIdentifiers =
                metadata.getBestRowIdentifier(
                    null, "public", "tracks", DatabaseMetaData.bestRowTransaction, false)) {
              assertTrue(rowIdentifiers.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataCatalogs() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet catalogs = metadata.getCatalogs()) {
              assertTrue(catalogs.next());
              assertNotNull(catalogs.getString("TABLE_CAT"));
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataSchemas() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet schemas = metadata.getSchemas()) {
              assertFalse(schemas.next());
            }

            try (ResultSet schemas = metadata.getSchemas(null, "public")) {
              assertFalse(schemas.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataClientInfoProperties() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet clientInfoProperties = metadata.getClientInfoProperties()) {
              // Ignore values, just check that it does not fail.
              clientInfoProperties.next();
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataFunctions() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet functions = metadata.getFunctions(null, null, null)) {
              assertTrue(functions.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataFunctionColumns() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet functionColumns = metadata.getFunctionColumns(null, null, null, null)) {
              assertTrue(functionColumns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataProcedures() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet procedures = metadata.getProcedures(null, null, null)) {
              assertTrue(procedures.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataProcedureColumns() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet procedureColumns =
                metadata.getProcedureColumns(null, null, null, null)) {
              assertTrue(procedureColumns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataPseudoColumns() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            metadata.getPseudoColumns(null, null, null, null);
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  @Ignore
  public void testDatabaseMetaDataVersionColumns() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet versionColumns = metadata.getVersionColumns(null, null, null)) {
              // Just ensure it does not fail, ignore any results.
              assertTrue(versionColumns.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataUDTs() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet types = metadata.getUDTs(null, null, null, null)) {
              assertFalse(types.next());
            }
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataMaxNameLength() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals(64, metadata.getMaxColumnNameLength());
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }

  @Test
  public void testDatabaseMetaDataSQLKeywords() throws Exception {
    runForAllVersions(
        (connection, version) -> {
          try {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue(metadata.getSQLKeywords().length() > 0);
          } catch (SQLException e) {
            throw SpannerExceptionFactory.asSpannerException(e);
          }
        });
  }
}