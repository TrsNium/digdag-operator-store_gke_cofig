package io.digdag.plugin.store_gke_config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.Plugin;
import io.digdag.client.config.ConfigFactory;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.List;

public class StoreGkeConfigPlugin implements Plugin {
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type) {
        if (type == OperatorProvider.class) {
            return StoreGkeConfigOperatorProvider.class.asSubclass(type);
        } else {
            return null;
        }
    }

    public static class StoreGkeConfigOperatorProvider implements OperatorProvider {
        @Inject
        protected CommandExecutor exec;
        @Inject
        protected ConfigFactory cf;

        @Override
        public List<OperatorFactory> get() {
            return Arrays.asList(new StoreGkeConfigOperatorFactory(exec, cf));
        }
    }
}
