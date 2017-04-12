package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.PositionReportModel;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class PositionReportVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(PositionReportVerticle.class);

    @Override
    protected void onConnect() {
        healthCheck.setComponentReady(Component.POSITION_REPORT);
    }

    @Override
    protected void onDisconnect() {
        healthCheck.setComponentFailed(Component.POSITION_REPORT);
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.positionReport)) {
                PrismaReports.PositionReport positionReportData = gpbObject.getExtension(PrismaReports.positionReport);
                try {
                    PositionReportModel positionReportModel = new PositionReportModel(header, positionReportData);
                    this.persistenceService.storePositionReport(positionReportModel, ar -> {
                        if (ar.succeeded()) {
                            LOG.debug("Position Report message processed");
                        } else {
                            LOG.error("Unable to store message", ar.cause());
                        }
                    });
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to create Position Report Model from GPB data", ex);
                }
            } else {
                LOG.error("Unknown extension (should be {})", PrismaReports.positionReport.getDescriptor().getName());
            }
        });
    }

}