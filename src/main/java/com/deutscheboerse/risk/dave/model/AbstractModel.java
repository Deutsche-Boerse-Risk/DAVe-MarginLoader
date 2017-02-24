package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractModel extends JsonObject implements MongoModel {

    public AbstractModel() {
    }

    public AbstractModel(PrismaReports.PrismaHeader header) {
        verify(header);

        put("snapshotID", header.getId());
        put("businessDate", header.getBusinessDate());
        Instant instant = Instant.ofEpochMilli(header.getTimestamp());
        put("timestamp", new JsonObject().put("$date", ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
    }

    private void verify(PrismaReports.PrismaHeader header) {
        checkArgument(header.hasId(), "Missing snapshot ID in header in AMQP data");
        checkArgument(header.hasBusinessDate(), "Missing business date in header in AMQP data");
        checkArgument(header.hasTimestamp(), "Missing timestamp in header in AMQP data");
    }

    @Override
    public JsonObject getHistoryUniqueIndex() {
        return new JsonObject().put("snapshotID", 1).mergeIn(getLatestUniqueIndex());
    }

}
