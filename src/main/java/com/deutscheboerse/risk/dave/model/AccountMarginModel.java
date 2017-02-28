package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject
public class AccountMarginModel extends AbstractModel {
    public AccountMarginModel() {
        super();
    }

    public AccountMarginModel(JsonObject json) {
        this.mergeIn(json);
    }

    public AccountMarginModel(AccountMarginModel other) {
        this.mergeIn(other);
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
    public Collection<String> getKeys() {
        List<String> keys = new ArrayList();
        keys.add("clearer");
        keys.add("member");
        keys.add("account");
        keys.add("marginCurrency");
        return Collections.unmodifiableCollection(keys);
    }
}
