package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.grpc.LiquiGroupMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class LiquiGroupMarginModel implements Model<LiquiGroupMargin> {

    private final LiquiGroupMargin grpc;

    public LiquiGroupMarginModel(JsonObject json) {
        verifyJson(json);
        this.grpc = ((GrpcJsonWrapper)json).toGpb(LiquiGroupMargin.class);
    }

    public LiquiGroupMarginModel(PrismaReports.PrismaHeader header, PrismaReports.LiquiGroupMargin data) {
        verifyPrismaHeader(header);
        verifyPrismaData(data);

        PrismaReports.LiquiGroupMarginKey key = data.getKey();
        this.grpc = LiquiGroupMargin.newBuilder()
                .setSnapshotId(header.getId())
                .setBusinessDate(header.getBusinessDate())
                .setTimestamp(header.getTimestamp())
                .setClearer(key.getClearer())
                .setMember(key.getMember())
                .setAccount(key.getAccount())
                .setMarginClass(key.getMarginClass())
                .setMarginCurrency(key.getMarginCurrency())
                .setMarginGroup(data.getMarginGroup())
                .setPremiumMargin(data.getPremiumMargin())
                .setCurrentLiquidatingMargin(data.getCurrentLiquidatingMargin())
                .setFuturesSpreadMargin(data.getFuturesSpreadMargin())
                .setAdditionalMargin(data.getAdditionalMargin())
                .setUnadjustedMarginRequirement(data.getUnadjustedMarginRequirement())
                .setVariationPremiumPayment(data.getVariationPremiumPayment())
                .build();
    }

    @Override
    public LiquiGroupMargin toGrpc() {
        return this.grpc;
    }

    private void verifyPrismaData(PrismaReports.LiquiGroupMargin liquiGroupMarginData) {
        checkArgument(liquiGroupMarginData.hasKey(), "Missing liqui group margin key in AMQP data");
        checkArgument(liquiGroupMarginData.getKey().hasClearer(), "Missing liqui group margin clearer in AMQP data");
        checkArgument(liquiGroupMarginData.getKey().hasMember(), "Missing liqui group margin member in AMQP data");
        checkArgument(liquiGroupMarginData.getKey().hasAccount(), "Missing liqui group margin account in AMQP data");
        checkArgument(liquiGroupMarginData.getKey().hasMarginClass(), "Missing liqui group margin class in AMQP data");
        checkArgument(liquiGroupMarginData.getKey().hasMarginCurrency(), "Missing liqui group margin margin currency in AMQP data");
        checkArgument(liquiGroupMarginData.hasPremiumMargin(), "Missing liqui group margin premium margin in AMQP data");
        checkArgument(liquiGroupMarginData.hasCurrentLiquidatingMargin(), "Missing liqui group current liquidating margin in AMQP data");
        checkArgument(liquiGroupMarginData.hasFuturesSpreadMargin(), "Missing liqui group futures spread margin in AMQP data");
        checkArgument(liquiGroupMarginData.hasAdditionalMargin(), "Missing liqui group additional margin in AMQP data");
        checkArgument(liquiGroupMarginData.hasUnadjustedMarginRequirement(), "Missing liqui group unadjusted margin in AMQP data");
        checkArgument(liquiGroupMarginData.hasVariationPremiumPayment(), "Missing liqui group variation premium payment in AMQP data");
    }
}
