/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.measurements.exporter;

import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Export measurements into InfluxDb
 */
public class InfluxDbMeasurementsExporter implements MeasurementsExporter {
  //  Property names
  public static final String USE_INFLUXDB_EXPORTER = "status.useinfluxdb";
  public static final String INFLUXDB_HOSTPORT = "status.influxdb.hostport";
  public static final String INFLUXDB_USERNAME = "status.influxdb.username";
  public static final String INFLUXDB_PASSWORD = "status.influxdb.password";
  public static final String INFLUXDB_DBNAME = "status.influxdb.dbname";
  public static final String INFLUXDB_TAGNAME = "status.influxdb.tagname";
  public static final String INFLUXDB_TAGVALUE = "status.influxdb.tagvalue";

  //  Consts
  private final String AUTOGEN = "autogen";
  private final int BATCH_POINTS_MAX = 2000;
  private final int BATCH_TIME_MAX_MS = 1000;

  // Connection fields
  private String hostPort;
  private String username = StringUtils.EMPTY;
  private String password = StringUtils.EMPTY;
  private String dbName;
  private String tagName = StringUtils.EMPTY;
  private String tagValue = StringUtils.EMPTY;

  private InfluxDB influxDB;

  public InfluxDbMeasurementsExporter(String hostPort, String username, String password, String dbName, String tagName,
                                      String tagValue) {
    this.hostPort = hostPort;
    this.username = username;
    this.password = password;
    this.dbName = dbName;
    this.tagName = tagName;
    this.tagValue = tagValue;

    influxDB = InfluxDBFactory.connect(hostPort, username, password);
    // Flush every X Points, at least every Y ms
    influxDB.enableBatch(BATCH_POINTS_MAX, BATCH_TIME_MAX_MS, TimeUnit.MILLISECONDS);
  }

  public void write(String metric, String measurement, int i) throws IOException {
    Point.Builder builder = Point.measurement(metric)
      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
      .addField(measurement, i);

    if (StringUtils.isNotEmpty(tagName)) {
      builder.tag(tagName, tagValue);
    }
    influxDB.write(dbName, AUTOGEN, builder.build());
  }

  public void write(String metric, String measurement, double d) throws IOException {
    Point.Builder builder = Point.measurement(metric)
      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
      .addField(measurement, d);

    if (StringUtils.isNotEmpty(tagName)) {
      builder.tag(tagName, tagValue);
    }
    influxDB.write(dbName, AUTOGEN, builder.build());
  }

  public void close() throws IOException {
    influxDB.close();
  }

}
