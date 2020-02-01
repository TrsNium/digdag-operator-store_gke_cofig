package io.digdag.plugin.store_gke_config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Paths;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StoreGkeConfigOperatorTest
{
    protected ObjectMapper mapper;
    protected ConfigFactory cf;
    protected CommandExecutor commandExecutor;
    protected OperatorContext operatorContext;

    @Before
    public void setUp()
            throws Exception
    {
        mapper = new ObjectMapper();
        cf = new ConfigFactory(mapper);
        commandExecutor = mock(CommandExecutor.class);
        operatorContext = mock(OperatorContext.class);
        TaskRequest taskRequest = mock(TaskRequest.class);

        Config taskRequestConfig = Config.deserializeFromJackson(this.mapper, this.mapper.createObjectNode());
        when(taskRequest.getConfig()).thenReturn(taskRequestConfig);

        when(operatorContext.getTaskRequest()).thenReturn(taskRequest);
        when(operatorContext.getProjectPath()).thenReturn(Paths.get("/testPath"));
    }

    @Test
    public void testGenerateChildTaskRequestConfig()
            throws Exception
    {
        final String testCluster = "test";
        final String namespace = "test";

        final StoreGkeConfigOperatorFactory storeGkeConfigOperatorFactory = new StoreGkeConfigOperatorFactory(this.commandExecutor, this.cf);
        final StoreGkeConfigOperatorFactory.StoreGkeConfigOperator operator = storeGkeConfigOperatorFactory.new StoreGkeConfigOperator(this.commandExecutor, this.cf, this.operatorContext);

        final Config generatedStoreParams = operator.generateStoreParams(testCluster, namespace);

        String kubeConfigPath = System.getenv("KUBECONFIG");
        if (kubeConfigPath == null) {
          kubeConfigPath = Paths.get(System.getenv("HOME"), ".kube/config").toString();
        }
        io.fabric8.kubernetes.client.Config kubeConfig = operator.getKubeConfigFromPath(kubeConfigPath);
        final Config desiredStoredParamsConfig = cf.create()
            .set("kubernetes", cf.create()
                .set("name", testCluster)
                .set("master", kubeConfig.getMasterUrl())
                .set("certs_ca_data", kubeConfig.getCaCertData())
                .set("oauth_token", kubeConfig.getOauthToken())
                .set("namespace", namespace));
        assertThat(generatedStoreParams, is(desiredStoredParamsConfig));
    }
}

