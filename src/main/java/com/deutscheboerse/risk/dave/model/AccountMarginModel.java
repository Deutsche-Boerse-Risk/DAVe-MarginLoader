package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.AccountMargin;
import com.google.protobuf.InvalidProtocolBufferException;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class AccountMarginModel implements Model<AccountMargin> {

    private final AccountMargin grpc;

    public AccountMarginModel(JsonObject json) {
        verifyJson(json);
        try {
            this.grpc = AccountMargin.parseFrom(json.getBinary("grpc"));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public AccountMarginModel(PrismaReports.PrismaHeader header, PrismaReports.AccountMargin data) {
        verifyPrismaHeader(header);
        verifyPrismaData(data);

        PrismaReports.AccountMarginKey key = data.getKey();
        this.grpc = AccountMargin.newBuilder()
                .setSnapshotId(header.getId())
                .setBusinessDate(header.getBusinessDate())
                .setTimestamp(header.getTimestamp())
                .setClearer(key.getClearer())
                .setMember(key.getMember())
                .setAccount(key.getAccount())
                .setMarginCurrency(key.getMarginCurrency())
                .setClearingCurrency(data.getClearingCurrency())
                .setPool(data.getPool())
                .setMarginReqInMarginCurr(data.getMarginReqInMarginCurr())
                .setMarginReqInClrCurr(data.getMarginReqInClrCurr())
                .setUnadjustedMarginRequirement(data.getUnadjustedMarginRequirement())
                .setVariationPremiumPayment(data.getVariationPremiumPayment())
                .build();
    }

    @Override
    public JsonObject toJson() {
        return new JsonObject().put("grpc", this.grpc.toByteArray());
    }

    @Override
    public AccountMargin toGrpc() {
        return this.grpc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof AccountMarginModel))
            return false;
        return this.grpc.equals(((AccountMarginModel) o).grpc);
    }

    @Override
    public int hashCode() {
        return this.grpc.hashCode();
    }

    private void verifyPrismaData(PrismaReports.AccountMargin data) {
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
