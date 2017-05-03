package com.deutscheboerse.risk.dave.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TestAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    private final List<String> classNames = new ArrayList<>();
    private final Map<Level, List<ILoggingEvent>> levelListMap = new ConcurrentHashMap<>();

    private TestAppender(String className, String... remainingNames) {
        this.classNames.add(className);
        this.classNames.addAll(Arrays.asList(remainingNames));
        this.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
    }

    public static TestAppender getAppender(Class<?> clazz) {
        return new TestAppender(clazz.getName());
    }

    public static TestAppender getAppender(Class<?> clazz1, Class<?> clazz2) {
        return new TestAppender(clazz1.getName(), clazz2.getName());
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (this.classNames.contains(event.getLoggerName())) {
            synchronized(this) {
                this.getList(event.getLevel()).add(event);
                this.notifyAll();
            }
        }
    }

    public synchronized void waitForMessageContains(Level level, String message) throws InterruptedException {
        while (!findFirst(level, message).isPresent()) {
            this.wait(5000);
        }
    }

    public synchronized void waitForMessageContains(Level level, String message, int count) throws InterruptedException {
        while (getList(level).size() < count || findAll(level, message).size() < count) {
            this.wait(5000);
        }
    }

    public synchronized void waitForMessageCount(Level level, int count) throws InterruptedException {
        while (getList(level).size() != count) {
            this.wait(5000);
        }
    }

    private Optional<ILoggingEvent> findFirst(Level level, String message) {
        return this.getList(level).stream()
                .filter(event -> event.getFormattedMessage().replace("\n", "").contains(message))
                .findFirst();
    }

    private List<ILoggingEvent> findAll(Level level, String message) {
        return this.getList(level).stream()
                .filter(event -> event.getFormattedMessage().replace("\n", "").contains(message))
                .collect(Collectors.toList());
    }

    private List<ILoggingEvent> getList(Level level) {
        return this.levelListMap.computeIfAbsent(level, i -> new ArrayList<>());
    }

    private void clear() {
        this.levelListMap.clear();
    }

    @Override
    public void stop() {
        this.clear();
        super.stop();
    }
}