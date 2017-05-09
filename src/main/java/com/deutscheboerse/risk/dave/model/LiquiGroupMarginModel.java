package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.LiquiGroupMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class LiquiGroupMarginModel extends AbstractModel<LiquiGroupMargin> {

    public LiquiGroupMarginModel(JsonObject json) {
        this.mergeIn(json);
    }

    public LiquiGroupMarginModel(PrismaReports.PrismaHeader header, PrismaReports.LiquiGroupMargin data) {
        super(header);

        verify(data);

        PrismaReports.LiquiGroupMarginKey key = data.getKey();
        put("clearer", key.getClearer());
        put("member", key.getMember());
        put("account", key.getAccount());
        put("marginClass", key.getMarginClass());
        put("marginCurrency", key.getMarginCurrency());

        put("marginGroup", data.getMarginGroup());
        put("premiumMargin", data.getPremiumMargin());
        put("currentLiquidatingMargin", data.getCurrentLiquidatingMargin());
        put("futuresSpreadMargin", data.getFuturesSpreadMargin());
        put("additionalMargin", data.getAdditionalMargin());
        put("unadjustedMarginRequirement", data.getUnadjustedMarginRequirement());
        put("variationPremiumPayment", data.getVariationPremiumPayment());
    }

    @Override
    public LiquiGroupMargin toGrpc() {
        return LiquiGroupMargin.newBuilder()
                .setSnapshotId(this.getInteger("snapshotID"))
                .setBusinessDate(this.getInteger("businessDate"))
                .setTimestamp(this.getLong("timestamp"))
                .setClearer(this.getString("clearer"))
                .setMember(this.getString("member"))
                .setAccount(this.getString("account"))
                .setMarginClass(this.getString("marginClass"))
                .setMarginCurrency(this.getString("marginCurrency"))
                .setMarginGroup(this.getString("marginGroup"))
                .setPremiumMargin(this.getDouble("premiumMargin"))
                .setCurrentLiquidatingMargin(this.getDouble("currentLiquidatingMargin"))
                .setFuturesSpreadMargin(this.getDouble("futuresSpreadMargin"))
                .setAdditionalMargin(this.getDouble("additionalMargin"))
                .setUnadjustedMarginRequirement(this.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(this.getDouble("variationPremiumPayment"))
                .build();
    }

    private void verify(PrismaReports.LiquiGroupMargin liquiGroupMarginData) {
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
