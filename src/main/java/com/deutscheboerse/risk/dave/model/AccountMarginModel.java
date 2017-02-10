package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.core.json.JsonObject;

public class AccountMarginModel extends AbstractModel {

    public static final String EB_STORE_ADDRESS = "AccountMarginStore";
    public static final String MONGO_HISTORY_COLLECTION = "AccountMargin";
    public static final String MONGO_LATEST_COLLECTION = "AccountMargin.latest";

    public AccountMarginModel() {
        super();
    }

    public AccountMarginModel(PrismaReports.PrismaHeader header, PrismaReports.AccountMargin accountMarginData) {
        super(header);

        verify(accountMarginData);

        this.setClearer(accountMarginData.getKey().getClearer());
        this.setMember(accountMarginData.getKey().getMember());
        this.setAccount(accountMarginData.getKey().getAccount());
        this.setMarginCurrency(accountMarginData.getKey().getMarginCurrency());
        this.setClearingCurrency(accountMarginData.getClearingCurrency());
        this.setPool(accountMarginData.getPool());
        this.setMarginReqInMarginCurr(accountMarginData.getMarginReqInMarginCurr());
        this.setMarginReqInCrlCurr(accountMarginData.getMarginReqInClrCurr());
        this.setUnadjustedMarginRequirement(accountMarginData.getUnadjustedMarginRequirement());
        this.setVariationPremiumPayment(accountMarginData.getVariationPremiumPayment());
    }

    private void verify(PrismaReports.AccountMargin accountMarginData) {
        if (!accountMarginData.hasKey()) throw new IllegalArgumentException("Missing account margin key in AMQP data");
        if (!accountMarginData.getKey().hasClearer()) throw new IllegalArgumentException("Missing account margin clearer in AMQP data");
        if (!accountMarginData.getKey().hasMember()) throw new IllegalArgumentException("Missing account margin member in AMQP data");
        if (!accountMarginData.getKey().hasAccount()) throw new IllegalArgumentException("Missing account margin account in AMQP data");
        if (!accountMarginData.getKey().hasMarginCurrency()) throw new IllegalArgumentException("Missing account margin margin currency in AMQP data");
        if (!accountMarginData.hasClearingCurrency()) throw new IllegalArgumentException("Missing account margin clearing currency in AMQP data");
        if (!accountMarginData.hasPool()) throw new IllegalArgumentException("Missing account margin pool in AMQP data");
        if (!accountMarginData.hasMarginReqInMarginCurr()) throw new IllegalArgumentException("Missing account margin margin requirement in margin currency in AMQP data");
        if (!accountMarginData.hasMarginReqInClrCurr()) throw new IllegalArgumentException("Missing account margin margin requirement in clearing currency in AMQP data");
        if (!accountMarginData.hasUnadjustedMarginRequirement()) throw new IllegalArgumentException("Missing account margin unadjusted margin requirement in clearing currency in AMQP data");
        if (!accountMarginData.hasVariationPremiumPayment()) throw new IllegalArgumentException("Missing account margin variation premium payment in AMQP data");
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
        queryParams.put("clearer", this.getClearer());
        queryParams.put("member", this.getMember());
        queryParams.put("account", this.getAccount());
        queryParams.put("marginCurrency", this.getMarginCurrency());
        return queryParams;
    }

    public String getClearer() {
        return getString("clearer");
    }

    public void setClearer(String clearer) {
        put("clearer", clearer);
    }

    public String getMember() {
        return getString("member");
    }

    public void setMember(String member) {
        put("member", member);
    }

    public String getAccount() {
        return getString("account");
    }

    public void setAccount(String account) {
        put("account", account);
    }

    public String getMarginCurrency() {
        return getString("marginCurrency");
    }

    public void setMarginCurrency(String currency) {
        put("marginCurrency", currency);
    }

    public String getClearingCurrency() {
        return getString("clearingCurrency");
    }

    public void setClearingCurrency(String currency) {
        put("clearingCurrency", currency);
    }

    public String getPool() {
        return getString("pool");
    }

    public void setPool(String pool) {
        put("pool", pool);
    }

    public double getMarginReqInMarginCurr() {
        return getDouble("marginReqInMarginCurr");
    }

    public void setMarginReqInMarginCurr(double req) {
        put("marginReqInMarginCurr", req);
    }

    public double getMarginReqInCrlCurr() {
        return getDouble("marginReqInCrlCurr");
    }

    public void setMarginReqInCrlCurr(double req) {
        put("marginReqInCrlCurr", req);
    }

    public double getUnadjustedMarginRequirement() {
        return getDouble("unadjustedMarginRequirement");
    }

    public void setUnadjustedMarginRequirement(double req) {
        put("unadjustedMarginRequirement", req);
    }

    public double getVariationPremiumPayment() {
        return getDouble("variationPremiumPayment");
    }

    public void setVariationPremiumPayment(double req) {
        put("variationPremiumPayment", req);
    }
}
