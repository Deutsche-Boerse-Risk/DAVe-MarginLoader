package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.AccountMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class AccountMarginModel extends AbstractModel<AccountMargin> {

    public AccountMarginModel(JsonObject json) {
        this.mergeIn(json);
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
        put("marginReqInClrCurr", data.getMarginReqInClrCurr());
        put("unadjustedMarginRequirement", data.getUnadjustedMarginRequirement());
        put("variationPremiumPayment", data.getVariationPremiumPayment());
    }

    @Override
    public AccountMargin toGrpc() {
        return AccountMargin.newBuilder()
                .setSnapshotId(this.getInteger("snapshotID"))
                .setBusinessDate(this.getInteger("businessDate"))
                .setTimestamp(this.getLong("timestamp"))
                .setClearer(this.getString("clearer"))
                .setMember(this.getString("member"))
                .setAccount(this.getString("account"))
                .setMarginCurrency(this.getString("marginCurrency"))
                .setClearingCurrency(this.getString("clearingCurrency"))
                .setPool(this.getString("pool"))
                .setMarginReqInMarginCurr(this.getDouble("marginReqInMarginCurr"))
                .setMarginReqInClrCurr(this.getDouble("marginReqInClrCurr"))
                .setUnadjustedMarginRequirement(this.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(this.getDouble("variationPremiumPayment"))
                .build();
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
}
