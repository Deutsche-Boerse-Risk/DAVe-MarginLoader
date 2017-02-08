package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class AbstractModel extends JsonObject {

    public AbstractModel() {
    }

    public AbstractModel(PrismaReports.PrismaHeader header) {
        if (!header.hasBusinessDate()) throw new IllegalArgumentException("Missing business date in header in AMQP data");
        if (!header.hasTimestamp()) throw new IllegalArgumentException("Missing timestamp in header in AMQP data");
        this.setBusinessDate(header.getBusinessDate());
        this.setTimestamp(header.getTimestamp());
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
