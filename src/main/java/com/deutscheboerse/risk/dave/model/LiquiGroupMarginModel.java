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

    public LiquiGroupMarginModel(PrismaReports.PrismaHeader header, PrismaReports.LiquiGroupMargin liquiGroupMarginData) {
        super(header);

        verify(liquiGroupMarginData);

        this.setClearer(liquiGroupMarginData.getKey().getClearer());
        this.setMember(liquiGroupMarginData.getKey().getMember());
        this.setAccount(liquiGroupMarginData.getKey().getAccount());
        this.setMarginClass(liquiGroupMarginData.getKey().getMarginClass());
        this.setMarginCurrency(liquiGroupMarginData.getKey().getMarginCurrency());
        this.setMarginGroup(liquiGroupMarginData.getMarginGroup());
        this.setPremiumMargin(liquiGroupMarginData.getPremiumMargin());
        this.setCurrentLiquidatingMargin(liquiGroupMarginData.getCurrentLiquidatingMargin());
        this.setFuturesSpreadMargin(liquiGroupMarginData.getFuturesSpreadMargin());
        this.setAdditionalMargin(liquiGroupMarginData.getAdditionalMargin());
        this.setUnadjustedMarginRequirement(liquiGroupMarginData.getUnadjustedMarginRequirement());
        this.setVariationPremiumPayment(liquiGroupMarginData.getVariationPremiumPayment());
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
        queryParams.put("clearer", this.getClearer());
        queryParams.put("member", this.getMember());
        queryParams.put("account", this.getAccount());
        queryParams.put("marginClass", this.getMarginClass());
        queryParams.put("marginCurrency", this.getMarginCurrency());
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

    public String getMarginClass() {
        return getString("marginClass");
    }

    public void setMarginClass(String marginClass) { put("marginClass", marginClass); }

    public String getMarginCurrency() {
        return getString("marginCurrency");
    }

    public void setMarginCurrency(String currency) {
        put("marginCurrency", currency);
    }

    public String getMarginGroup() {
        return getString("marginGroup");
    }

    public void setMarginGroup(String group) {
        put("marginGroup", group);
    }

    public double getPremiumMargin() {
        return getDouble("premiumMargin");
    }

    public void setPremiumMargin(double margin) {
        put("premiumMargin", margin);
    }

    public double getCurrentLiquidatingMargin() {
        return getDouble("currentLiquidatingMargin");
    }

    public void setCurrentLiquidatingMargin(double margin) {
        put("currentLiquidatingMargin", margin);
    }

    public double getFuturesSpreadMargin() {
        return getDouble("futuresSpreadMargin");
    }

    public void setFuturesSpreadMargin(double margin) {
        put("futuresSpreadMargin", margin);
    }

    public double getAdditionalMargin() {
        return getDouble("additionalMargin");
    }

    public void setAdditionalMargin(double margin) {
        put("additionalMargin", margin);
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
