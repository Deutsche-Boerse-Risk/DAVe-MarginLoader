package com.deutscheboerse.risk.dave.utils;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.*;
import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DataHelper.class);

    public static final String ACCOUNT_MARGIN_FOLDER = "accountMargin";
    public static final String LIQUI_GROUP_MARGIN_FOLDER = "liquiGroupMargin";
    public static final String LIQUI_GROUP_SPLIT_MARGIN_FOLDER = "liquiGroupSplitMargin";
    public static final String POOL_MARGIN_FOLDER = "poolMargin";
    public static final String POSITION_REPORT_FOLDER = "positionReport";
    public static final String RISK_LIMIT_UTILIZATION_FOLDER = "riskLimitUtilization";

    private static Optional<JsonArray> getJsonArrayFromTTSaveFile(String folderName, int ttsaveNo) {
        String jsonPath = String.format("%s/snapshot-%03d.json", MainVerticleIT.class.getResource(folderName).getPath(), ttsaveNo);
        try {
            byte[] jsonArrayBytes = Files.readAllBytes(Paths.get(jsonPath));
            JsonArray jsonArray = new JsonArray(new String(jsonArrayBytes, Charset.defaultCharset()));
            return Optional.of(jsonArray);
        } catch (IOException e) {
            LOG.error("Unable to read data from {}", jsonPath, e);
            return Optional.empty();
        }
    }

    public static void readTTSaveFile(String folderName, int ttsaveNo, Consumer<JsonObject> consumer) {
        getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .forEach(json -> consumer.accept((JsonObject) json));
    }

    public static Collection<JsonObject> readTTSaveFile(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .map(json -> (JsonObject) json)
                .collect(Collectors.toList());
    }

    public static <T extends Model> List<T> readTTSaveFile(
            String folderName, int ttsaveNo, Function<JsonObject, T> modelFactory) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .map(json -> modelFactory.apply((JsonObject) json))
                .collect(Collectors.toList());
    }

    public static Optional<JsonObject> getLastJsonFromFile(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .map(json -> (JsonObject) json)
                .reduce((a, b) -> b);
    }

    public static <T extends Model> T getLastModelFromFile(String folderName, int ttsaveNo,
                                                           Function<JsonObject, T> modelFactory) {
        return modelFactory.apply(
                getLastJsonFromFile(folderName, ttsaveNo).orElse(new JsonObject()));
    }

    public static int getJsonObjectCount(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .size();
    }

    public static PrismaReports.AccountMargin createPrismaAccountMarginFromJson(JsonObject json) {
        return PrismaReports.AccountMargin.newBuilder()
                .setKey(PrismaReports.AccountMarginKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setAccount(json.getString("account"))
                        .setMarginCurrency(json.getString("marginCurrency")))
                .setClearingCurrency(json.getString("clearingCurrency"))
                .setPool(json.getString("pool"))
                .setMarginReqInMarginCurr(json.getDouble("marginReqInMarginCurr"))
                .setMarginReqInClrCurr(json.getDouble("marginReqInClrCurr"))
                .setUnadjustedMarginRequirement(json.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build();
    }

    public static PrismaReports.LiquiGroupMargin createPrismaLiquiGroupMarginFromJson(JsonObject json) {
        return PrismaReports.LiquiGroupMargin.newBuilder()
                .setKey(PrismaReports.LiquiGroupMarginKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setAccount(json.getString("account"))
                        .setMarginClass(json.getString("marginClass"))
                        .setMarginCurrency(json.getString("marginCurrency")))
                .setMarginGroup(json.getString("marginGroup"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setCurrentLiquidatingMargin(json.getDouble("currentLiquidatingMargin"))
                .setFuturesSpreadMargin(json.getDouble("futuresSpreadMargin"))
                .setAdditionalMargin(json.getDouble("additionalMargin"))
                .setUnadjustedMarginRequirement(json.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build();
    }

    public static PrismaReports.LiquiGroupSplitMargin createPrismaLiquiGroupSplitMarginFromJson(JsonObject json) {
        return PrismaReports.LiquiGroupSplitMargin.newBuilder()
                .setKey(PrismaReports.LiquiGroupSplitMarginKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setAccount(json.getString("account"))
                        .setLiquidationGroup(json.getString("liquidationGroup"))
                        .setLiquidationGroupSplit(json.getString("liquidationGroupSplit"))
                        .setMarginCurrency(json.getString("marginCurrency")))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setMarketRisk(json.getDouble("marketRisk"))
                .setLiquRisk(json.getDouble("liquRisk"))
                .setLongOptionCredit(json.getDouble("longOptionCredit"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build();
    }

    public static PrismaReports.PoolMargin createPrismaPoolMarginFromJson(JsonObject json) {
        return PrismaReports.PoolMargin.newBuilder()
                .setKey(PrismaReports.PoolMarginKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setPool(json.getString("pool"))
                        .setMarginCurrency(json.getString("marginCurrency")))
                .setClrRptCurrency(json.getString("clrRptCurrency"))
                .setRequiredMargin(json.getDouble("requiredMargin"))
                .setCashCollateralAmount(json.getDouble("cashCollateralAmount"))
                .setAdjustedSecurities(json.getDouble("adjustedSecurities"))
                .setAdjustedGuarantee(json.getDouble("adjustedGuarantee"))
                .setOverUnderInMarginCurr(json.getDouble("overUnderInMarginCurr"))
                .setOverUnderInClrRptCurr(json.getDouble("overUnderInClrRptCurr"))
                .setVariPremInMarginCurr(json.getDouble("variPremInMarginCurr"))
                .setAdjustedExchangeRate(json.getDouble("adjustedExchangeRate"))
                .setPoolOwner(json.getString("poolOwner"))
                .build();
    }

    public static PrismaReports.PositionReport createPrismaPositionReportFromJson(JsonObject json) {
        return PrismaReports.PositionReport.newBuilder()
                .setKey(PrismaReports.PositionReportKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setAccount(json.getString("account"))
                        .setLiquidationGroup(json.getString("liquidationGroup"))
                        .setLiquidationGroupSplit(json.getString("liquidationGroupSplit"))
                        .setProduct(json.getString("product"))
                        .setCallPut(json.getString("callPut"))
                        .setContractYear(json.getInteger("contractYear"))
                        .setContractMonth(json.getInteger("contractMonth"))
                        .setExpiryDay(json.getInteger("expiryDay"))
                        .setExercisePrice(json.getDouble("exercisePrice"))
                        .setVersion(json.getString("version"))
                        .setFlexContractSymbol(json.getString("flexContractSymbol"))
                )
                .setNetQuantityLs(json.getDouble("netQuantityLs"))
                .setNetQuantityEa(json.getDouble("netQuantityEa"))
                .setClearingCurrency(json.getString("clearingCurrency"))
                .setMVar(json.getDouble("mVar"))
                .setCompVar(json.getDouble("compVar"))
                .setCompCorrelationBreak(json.getDouble("compCorrelationBreak"))
                .setCompCompressionError(json.getDouble("compCompressionError"))
                .setCompLiquidityAddOn(json.getDouble("compLiquidityAddOn"))
                .setCompLongOptionCredit(json.getDouble("compLongOptionCredit"))
                .setProductCurrency(json.getString("productCurrency"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setNormalizedDelta(json.getDouble("normalizedDelta"))
                .setNormalizedGamma(json.getDouble("normalizedGamma"))
                .setNormalizedVega(json.getDouble("normalizedVega"))
                .setNormalizedRho(json.getDouble("normalizedRho"))
                .setNormalizedTheta(json.getDouble("normalizedTheta"))
                .setUnderlying(json.getString("underlying"))
                .build();
    }

    public static PrismaReports.RiskLimitUtilization createPrismaRiskLimitUtilizationFromJson(JsonObject json) {
        PrismaReports.RiskLimitUtilization.Builder builder = PrismaReports.RiskLimitUtilization.newBuilder()
                .setKey(PrismaReports.RiskLimitUtilizationKey.newBuilder()
                        .setClearer(json.getString("clearer"))
                        .setMember(json.getString("member"))
                        .setMaintainer(json.getString("maintainer"))
                        .setLimitType(json.getString("limitType")))
                .setUtilization(json.getDouble("utilization"));
        if (json.containsKey("warningLevel")) {
            builder.setWarningLevel(json.getDouble("warningLevel"));
        }
        if (json.containsKey("throttleLevel")) {
            builder.setThrottleLevel(json.getDouble("throttleLevel"));
        }
        if (json.containsKey("rejectLevel")) {
            builder.setRejectLevel(json.getDouble("rejectLevel"));
        }
        return builder.build();
    }

    public static PrismaReports.PrismaHeader createPrismaHeaderFromJson(JsonObject json) {
        return PrismaReports.PrismaHeader.newBuilder()
                .setId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .build();
    }

    public static AccountMarginModel createAccountMarginModelFromJson(JsonObject json) {
        return new AccountMarginModel(new JsonObject().put("grpc", AccountMargin.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setAccount(json.getString("account"))
                .setMarginCurrency(json.getString("marginCurrency"))
                .setClearingCurrency(json.getString("clearingCurrency"))
                .setPool(json.getString("pool"))
                .setMarginReqInMarginCurr(json.getDouble("marginReqInMarginCurr"))
                .setMarginReqInClrCurr(json.getDouble("marginReqInClrCurr"))
                .setUnadjustedMarginRequirement(json.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build().toByteArray()));
    }

    public static LiquiGroupMarginModel createLiquiGroupMarginModelFromJson(JsonObject json) {
        return new LiquiGroupMarginModel(new JsonObject().put("grpc", LiquiGroupMargin.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setAccount(json.getString("account"))
                .setMarginClass(json.getString("marginClass"))
                .setMarginCurrency(json.getString("marginCurrency"))
                .setMarginGroup(json.getString("marginGroup"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setCurrentLiquidatingMargin(json.getDouble("currentLiquidatingMargin"))
                .setFuturesSpreadMargin(json.getDouble("futuresSpreadMargin"))
                .setAdditionalMargin(json.getDouble("additionalMargin"))
                .setUnadjustedMarginRequirement(json.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build().toByteArray()));
    }

    public static LiquiGroupSplitMarginModel createLiquiGroupSplitMarginModelFromJson(JsonObject json) {
        return new LiquiGroupSplitMarginModel(new JsonObject().put("grpc", LiquiGroupSplitMargin.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setAccount(json.getString("account"))
                .setLiquidationGroup(json.getString("liquidationGroup"))
                .setLiquidationGroupSplit(json.getString("liquidationGroupSplit"))
                .setMarginCurrency(json.getString("marginCurrency"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setMarketRisk(json.getDouble("marketRisk"))
                .setLiquRisk(json.getDouble("liquRisk"))
                .setLongOptionCredit(json.getDouble("longOptionCredit"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build().toByteArray()));
    }

    public static PoolMarginModel createPoolMarginModelFromJson(JsonObject json) {
        return new PoolMarginModel(new JsonObject().put("grpc", PoolMargin.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setPool(json.getString("pool"))
                .setMarginCurrency(json.getString("marginCurrency"))
                .setClrRptCurrency(json.getString("clrRptCurrency"))
                .setRequiredMargin(json.getDouble("requiredMargin"))
                .setCashCollateralAmount(json.getDouble("cashCollateralAmount"))
                .setAdjustedSecurities(json.getDouble("adjustedSecurities"))
                .setAdjustedGuarantee(json.getDouble("adjustedGuarantee"))
                .setOverUnderInMarginCurr(json.getDouble("overUnderInMarginCurr"))
                .setOverUnderInClrRptCurr(json.getDouble("overUnderInClrRptCurr"))
                .setVariPremInMarginCurr(json.getDouble("variPremInMarginCurr"))
                .setAdjustedExchangeRate(json.getDouble("adjustedExchangeRate"))
                .setPoolOwner(json.getString("poolOwner"))
                .build().toByteArray()));
    }

    public static PositionReportModel createPositionReportModelFromJson(JsonObject json) {
        return new PositionReportModel(new JsonObject().put("grpc", PositionReport.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setAccount(json.getString("account"))
                .setLiquidationGroup(json.getString("liquidationGroup"))
                .setLiquidationGroupSplit(json.getString("liquidationGroupSplit"))
                .setProduct(json.getString("product"))
                .setCallPut(json.getString("callPut"))
                .setContractYear(json.getInteger("contractYear"))
                .setContractMonth(json.getInteger("contractMonth"))
                .setExpiryDay(json.getInteger("expiryDay"))
                .setExercisePrice(json.getDouble("exercisePrice"))
                .setVersion(json.getString("version"))
                .setFlexContractSymbol(json.getString("flexContractSymbol"))
                .setNetQuantityLs(json.getDouble("netQuantityLs"))
                .setNetQuantityEa(json.getDouble("netQuantityEa"))
                .setClearingCurrency(json.getString("clearingCurrency"))
                .setMVar(json.getDouble("mVar"))
                .setCompVar(json.getDouble("compVar"))
                .setCompCorrelationBreak(json.getDouble("compCorrelationBreak"))
                .setCompCompressionError(json.getDouble("compCompressionError"))
                .setCompLiquidityAddOn(json.getDouble("compLiquidityAddOn"))
                .setCompLongOptionCredit(json.getDouble("compLongOptionCredit"))
                .setProductCurrency(json.getString("productCurrency"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setNormalizedDelta(json.getDouble("normalizedDelta"))
                .setNormalizedGamma(json.getDouble("normalizedGamma"))
                .setNormalizedVega(json.getDouble("normalizedVega"))
                .setNormalizedRho(json.getDouble("normalizedRho"))
                .setNormalizedTheta(json.getDouble("normalizedTheta"))
                .setUnderlying(json.getString("underlying"))
                .build().toByteArray()));
    }

    public static RiskLimitUtilizationModel createRiskLimitUtilizationModelFromJson(JsonObject json) {
        return new RiskLimitUtilizationModel(new JsonObject().put("grpc", RiskLimitUtilization.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setMaintainer(json.getString("maintainer"))
                .setLimitType(json.getString("limitType"))
                .setUtilization(json.getDouble("utilization"))
                .setWarningLevel(json.getDouble("warningLevel", 0.0))
                .setThrottleLevel(json.getDouble("throttleLevel", 0.0))
                .setRejectLevel(json.getDouble("rejectLevel", 0.0))
                .build().toByteArray()));
    }
}
