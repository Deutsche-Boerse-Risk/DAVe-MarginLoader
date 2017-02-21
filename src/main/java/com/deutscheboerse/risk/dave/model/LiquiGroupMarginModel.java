package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

public class LiquiGroupMarginModel extends AbstractModel {
    public static final String MONGO_HISTORY_COLLECTION = "LiquiGroupMargin";
    public static final String MONGO_LATEST_COLLECTION = "LiquiGroupMargin.latest";

    public LiquiGroupMarginModel() {
        super();
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

    @Override
    public String getHistoryCollection() {
        return LiquiGroupMarginModel.MONGO_HISTORY_COLLECTION;
    }

    @Override
    public String getLatestCollection() {
        return LiquiGroupMarginModel.MONGO_LATEST_COLLECTION;
    }

    @Override
    public JsonObject getLatestQueryParams() {
        JsonObject queryParams = new JsonObject();
        queryParams.put("clearer", getString("clearer"));
        queryParams.put("member", getString("member"));
        queryParams.put("account", getString("account"));
        queryParams.put("marginClass", getString("marginClass"));
        queryParams.put("marginCurrency", getString("marginCurrency"));
        return queryParams;
    }

    @Override
    public JsonObject getLatestUniqueIndex() {
        return new JsonObject()
                .put("clearer", 1)
                .put("member", 1)
                .put("account", 1)
                .put("marginClass", 1)
                .put("marginCurrency", 1);
    }

}
