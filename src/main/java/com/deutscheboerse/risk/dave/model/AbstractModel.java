package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractModel extends JsonObject {

    public AbstractModel() {
    }

    public AbstractModel(PrismaReports.PrismaHeader header) {
        verify(header);

        put("snapshotID", header.getId());
        put("businessDate", header.getBusinessDate());
        put("timestamp", header.getTimestamp());
    }

    public JsonObject toJson() {
        return new JsonObject(this.getMap());
    }

    private void verify(PrismaReports.PrismaHeader header) {
        checkArgument(header.hasId(), "Missing snapshot ID in header in AMQP data");
        checkArgument(header.hasBusinessDate(), "Missing business date in header in AMQP data");
        checkArgument(header.hasTimestamp(), "Missing timestamp in header in AMQP data");
    }

    public abstract Collection<String> getKeys();
}
