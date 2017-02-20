package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public abstract class AbstractModel extends JsonObject implements MongoModel {

    public AbstractModel() {
    }

    public AbstractModel(PrismaReports.PrismaHeader header) {
        verify(header);
        this.setSnapshotID(header.getId());
        this.setBusinessDate(header.getBusinessDate());
        this.setTimestamp(header.getTimestamp());
    }

    private void verify(PrismaReports.PrismaHeader header) {
        if (!header.hasId()) throw new IllegalArgumentException("Missing snapshot ID in header in AMQP data");
        if (!header.hasBusinessDate()) throw new IllegalArgumentException("Missing business date in header in AMQP data");
        if (!header.hasTimestamp()) throw new IllegalArgumentException("Missing timestamp in header in AMQP data");
    }

    @Override
    public JsonObject getHistoryUniqueIndex() {
        return new JsonObject().put("snapshotID", 1).mergeIn(getLatestUniqueIndex());
    }

    public int getSnapshotID() {
        return getInteger("snapshotID");
    }

    public void setSnapshotID(int snapshotID) {
        put("snapshotID", snapshotID);
    }

    public int getBusinessDate() {
        return getInteger("businessDate");
    }

    public void setBusinessDate(int businessDate) {
        put("businessDate", businessDate);
    }

    public JsonObject getTimestamp() {
        return getJsonObject("timestamp");
    }

    public void setTimestamp(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        put("timestamp", new JsonObject().put("$date", ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
    }
}
