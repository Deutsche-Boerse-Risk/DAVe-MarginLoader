package com.deutscheboerse.risk.dave.model;

import com.google.protobuf.MessageLite;
import io.vertx.core.json.JsonObject;

import java.util.Objects;

public class GrpcJsonWrapper extends JsonObject {
    private final MessageLite gpb;

    public GrpcJsonWrapper(MessageLite gpb) {
        Objects.requireNonNull(gpb);
        this.gpb = gpb;
    }

    public <T extends MessageLite> T toGpb(Class<T> clazz) {
        return clazz.cast(this.gpb);
    }

    @Override
    public GrpcJsonWrapper copy() {
        return this;
    }
}
