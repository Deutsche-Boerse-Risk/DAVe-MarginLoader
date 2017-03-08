package com.deutscheboerse.risk.dave.utils;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Optional;
import java.util.function.Function;

public class BrokerFillerMissingField extends BrokerFillerCorrectData {

    public BrokerFillerMissingField(Vertx vertx) {
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
        ObjectList.GPBHeader gpbHeader = ObjectList.GPBHeader.newBuilder()
                .setExtension(PrismaReports.prismaHeader, BrokerFillerMissingField.createWrongPrismaHeaderFromJson(lastRecord)).build();
        gpbObjectListBuilder.setHeader(gpbHeader);
        return Optional.of(gpbObjectListBuilder.build());
    };

    private static PrismaReports.PrismaHeader createWrongPrismaHeaderFromJson(JsonObject json) {
        PrismaReports.PrismaHeader result = PrismaReports.PrismaHeader.newBuilder()
                .setId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                //.setTimestamp(json.getLong("timestamp"))
                .build();
        return result;
    }

}
