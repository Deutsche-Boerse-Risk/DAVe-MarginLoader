package com.deutscheboerse.risk.dave.log;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class TestAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    private String className;
    private ILoggingEvent lastLogMessage;
    private AtomicInteger messageCount = new AtomicInteger(0);

    private TestAppender(String className) {
        this.className = className;
        this.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
    }

    public static TestAppender getAppender(final Class<?> clazz) {
        return new TestAppender(clazz.getName());
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (event.getLoggerName().equals(this.className)) {
            synchronized(this) {
                lastLogMessage = event;
                this.messageCount.incrementAndGet();
                this.notifyAll();
            }
        }
    }

    public ILoggingEvent getLastMessage() throws InterruptedException {
        synchronized(this) {
            while (this.lastLogMessage == null) {
                this.wait(5000);
            }
        }
        return lastLogMessage;
    }

    public void waitForMessageCount(int count) throws InterruptedException {
        synchronized(this) {
            while (this.messageCount.intValue() < count) {
                this.wait(5000);
            }
        }
    }

    public void waitForMessageContains(String message) throws InterruptedException {
        synchronized(this) {
            while (!this.getLastMessage().getFormattedMessage().contains(message)) {
                this.wait(5000);
            }
        }
    }

    @Override
    public void stop() {
        lastLogMessage = null;
        super.stop();
    }
}