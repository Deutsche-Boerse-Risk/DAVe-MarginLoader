package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

public class AccountMarginModel extends AbstractModel {
    public static final String MONGO_HISTORY_COLLECTION = "AccountMargin";
    public static final String MONGO_LATEST_COLLECTION = "AccountMargin.latest";

    public AccountMarginModel() {
        super();
    }

    public AccountMarginModel(PrismaReports.PrismaHeader header, PrismaReports.AccountMargin data) {
        super(header);

        verify(data);

        PrismaReports.AccountMarginKey key = data.getKey();
        put("clearer", key.getClearer());
        put("member", key.getMember());
        put("account", key.getAccount());
        put("marginCurrency", key.getMarginCurrency());
        put("clearingCurrency", data.getClearingCurrency());
        put("pool", data.getPool());
        put("marginReqInMarginCurr", data.getMarginReqInMarginCurr());
        put("marginReqInCrlCurr", data.getMarginReqInClrCurr());
        put("unadjustedMarginRequirement", data.getUnadjustedMarginRequirement());
        put("variationPremiumPayment", data.getVariationPremiumPayment());
    }

    private void verify(PrismaReports.AccountMargin data) {
        checkArgument(data.hasKey(), "Missing account margin key in AMQP data");
        checkArgument(data.getKey().hasClearer(), "Missing account margin clearer in AMQP data");
        checkArgument(data.getKey().hasMember(), "Missing account margin member in AMQP data");
        checkArgument(data.getKey().hasAccount(), "Missing account margin account in AMQP data");
        checkArgument(data.getKey().hasMarginCurrency(), "Missing account margin margin currency in AMQP data");
        checkArgument(data.hasClearingCurrency(), "Missing account margin clearing currency in AMQP data");
        checkArgument(data.hasPool(), "Missing account margin pool in AMQP data");
        checkArgument(data.hasMarginReqInMarginCurr(), "Missing account margin margin requirement in margin currency in AMQP data");
        checkArgument(data.hasMarginReqInClrCurr(), "Missing account margin margin requirement in clearing currency in AMQP data");
        checkArgument(data.hasUnadjustedMarginRequirement(), "Missing account margin unadjusted margin requirement in clearing currency in AMQP data");
        checkArgument(data.hasVariationPremiumPayment(), "Missing account margin variation premium payment in AMQP data");
    }

    @Override
    public String getHistoryCollection() {
        return AccountMarginModel.MONGO_HISTORY_COLLECTION;
    }

    @Override
    public String getLatestCollection() {
        return AccountMarginModel.MONGO_LATEST_COLLECTION;
    }

    @Override
    public JsonObject getLatestQueryParams() {
        JsonObject queryParams = new JsonObject();
        queryParams.put("clearer", getString("clearer"));
        queryParams.put("member", getString("member"));
        queryParams.put("account", getString("account"));
        queryParams.put("marginCurrency", getString("marginCurrency"));
        return queryParams;
    }

    @Override
    public JsonObject getLatestUniqueIndex() {
        return new JsonObject()
                .put("clearer", 1)
                .put("member", 1)
                .put("account", 1)
                .put("marginCurrency", 1);
    }

}
