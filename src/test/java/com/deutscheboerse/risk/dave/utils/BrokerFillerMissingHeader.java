package com.deutscheboerse.risk.dave.utils;

import CIL.ObjectList;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Optional;
import java.util.function.Function;

public class BrokerFillerMissingHeader extends BrokerFillerCorrectData {

    public BrokerFillerMissingHeader(Vertx vertx) {
        super(vertx);
    }

    @Override
    protected Optional<ObjectList.GPBObjectList> createGPBFromJson(String folderName, int ttsaveNo, Function<JsonObject, ObjectList.GPBObject> creator) {
        ObjectList.GPBObjectList.Builder gpbObjectListBuilder = ObjectList.GPBObjectList.newBuilder();
        JsonObject lastRecord = new JsonObject();
        DataHelper.readTTSaveFile(folderName, ttsaveNo).forEach(json -> {
            ObjectList.GPBObject gpbObject = creator.apply(json);
            gpbObjectListBuilder.addItem(gpbObject);
            lastRecord.mergeIn(json);
        });
        if (lastRecord.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(gpbObjectListBuilder.build());
    }

}
