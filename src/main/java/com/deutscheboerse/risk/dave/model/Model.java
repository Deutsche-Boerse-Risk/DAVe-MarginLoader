package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.google.protobuf.MessageLite;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

public interface Model<T extends MessageLite> {

    JsonObject toJson();
    T toGrpc();

    default void verifyPrismaHeader(PrismaReports.PrismaHeader header) {
        checkArgument(header.hasId(), "Missing snapshot ID in header in AMQP data");
        checkArgument(header.hasBusinessDate(), "Missing business date in header in AMQP data");
        checkArgument(header.hasTimestamp(), "Missing timestamp in header in AMQP data");
    }

    default void verifyJson(JsonObject json) {
        if (!json.containsKey("grpc")) {
            throw new IllegalArgumentException("Expected grpc field");
        }
    }
}
