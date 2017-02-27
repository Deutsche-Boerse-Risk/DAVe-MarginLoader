package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.persistence.MongoPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.google.inject.AbstractModule;

import javax.inject.Singleton;

public class Binder extends AbstractModule {

    @Override
    protected void configure() {
        bind(PersistenceService.class).to(MongoPersistenceService.class).in(Singleton.class);
    }
}
