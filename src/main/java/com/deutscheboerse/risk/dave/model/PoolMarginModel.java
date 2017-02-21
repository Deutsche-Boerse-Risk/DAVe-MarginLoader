package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

public class PoolMarginModel extends AbstractModel {
    public static final String EB_STORE_ADDRESS = "PoolMarginStore";
    public static final String MONGO_HISTORY_COLLECTION = "PoolMargin";
    public static final String MONGO_LATEST_COLLECTION = "PoolMargin.latest";

    public PoolMarginModel() {
        super();
    }

    public PoolMarginModel(PrismaReports.PrismaHeader header, PrismaReports.PoolMargin data) {
        super(header);

        verify(data);

        PrismaReports.PoolMarginKey key = data.getKey();
        put("clearer", key.getClearer());
        put("pool", key.getPool());
        put("marginCurrency", key.getMarginCurrency());
        put("clrRptCurrency", data.getClrRptCurrency());
        put("requiredMargin", data.getRequiredMargin());
        put("cashCollateralAmount", data.getCashCollateralAmount());
        put("adjustedSecurities", data.getAdjustedSecurities());
        put("adjustedGuarantee", data.getAdjustedGuarantee());
        put("overUnderInMarginCurr", data.getOverUnderInMarginCurr());
        put("overUnderInClrRptCurr", data.getOverUnderInClrRptCurr());
        put("variPremInMarginCurr", data.getVariPremInMarginCurr());
        put("adjustedExchangeRate", data.getAdjustedExchangeRate());
        put("poolOwner", data.getPoolOwner());
    }

    private void verify(PrismaReports.PoolMargin data) {
        checkArgument(data.hasKey(), "Missing pool key in AMQP data");
        checkArgument(data.getKey().hasClearer(), "Missing pool clearer in AMQP data");
        checkArgument(data.getKey().hasPool(), "Missing pool name in AMQP data");
        checkArgument(data.getKey().hasMarginCurrency(), "Missing pool margin currency in AMQP data");
        checkArgument(data.hasClrRptCurrency(), "Missing pool reporting currency in AMQP data");
        checkArgument(data.hasRequiredMargin(), "Missing pool required margin in AMQP data");
        checkArgument(data.hasCashCollateralAmount(), "Missing pool cash collateral amount in AMQP data");
        checkArgument(data.hasAdjustedSecurities(), "Missing pool adjusted securities in AMQP data");
        checkArgument(data.hasAdjustedGuarantee(), "Missing pool adjusted guarantee in AMQP data");
        checkArgument(data.hasOverUnderInMarginCurr(), "Missing pool over/under in margin currency in AMQP data");
        checkArgument(data.hasOverUnderInClrRptCurr(), "Missing pool over/under in reporting currency in AMQP data");
        checkArgument(data.hasVariPremInMarginCurr(), "Missing pool variation premium in AMQP data");
        checkArgument(data.hasAdjustedExchangeRate(), "Missing pool adjusted exchange rate in AMQP data");
        checkArgument(data.hasPoolOwner(), "Missing pool owner in AMQP data");
    }

    @Override
    public String getHistoryCollection() {
        return PoolMarginModel.MONGO_HISTORY_COLLECTION;
    }

    @Override
    public String getLatestCollection() {
        return PoolMarginModel.MONGO_LATEST_COLLECTION;
    }

    @Override
    public JsonObject getLatestQueryParams() {
        return new JsonObject()
            .put("clearer", getString("clearer"))
            .put("pool", getString("pool"))
            .put("marginCurrency", getString("marginCurrency"));
    }

    @Override
    public JsonObject getLatestUniqueIndex() {
        return new JsonObject()
                .put("clearer", 1)
                .put("pool", 1)
                .put("marginCurrency", 1);
    }
}
