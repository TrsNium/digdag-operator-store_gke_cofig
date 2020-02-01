package io.digdag.plugin.store_gke_config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.ImmutableTaskRequest;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.SecretNotFoundException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.PyOperatorFactory;
import io.digdag.standards.operator.RbOperatorFactory;
import io.digdag.standards.operator.ShOperatorFactory;
import io.digdag.util.BaseOperator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StoreGkeConfigOperatorFactory implements OperatorFactory {

    private final CommandExecutor exec;
    private final ConfigFactory cf;

    public StoreGkeConfigOperatorFactory(CommandExecutor exec, ConfigFactory cf) {
        this.exec = exec;
        this.cf = cf;
    }

    @Override
    public String getType() {
        return "store_gke_config";
    }

    @Override
    public Operator newOperator(OperatorContext context) {
        return new StoreGkeConfigOperator(this.exec, this.cf, context);
    }

    @VisibleForTesting
    class StoreGkeConfigOperator extends BaseOperator {

        private final CommandExecutor exec;
        private final ConfigFactory cf;
        private final OperatorContext context;
        StoreGkeConfigOperator(CommandExecutor exec, ConfigFactory cf, OperatorContext context) {
            super(context);
            this.context = context;
            this.exec = exec;
            this.cf = cf;
        }

        @Override
        public TaskResult runTask() {
            Config commandRequestConfig = request.getConfig()
                .getNestedOrGetEmpty("store_gke_config")
                .getNestedOrGetEmpty("_command");

            String cluster = commandRequestConfig.get("cluster", String.class);
            String project_id = commandRequestConfig.get("project_id", String.class);
            String zone = commandRequestConfig.get("zone", String.class);
            String namespace = commandRequestConfig.get("namespace", String.class, "default");

            if (commandRequestConfig.has("credential_json") || commandRequestConfig.has("credential_json_path") || commandRequestConfig.has("credential_json_from_secret_key")) {
                authCLI(commandRequestConfig);
            }

            // Auth GKECluster master with CLI
            String authGkeCommand = String.format("gcloud container clusters get-credentials %s --zone %s --project %s && kubectl get po && kubectl config set-context --current --namespace=%s", cluster, zone, project_id, namespace);
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", authGkeCommand);
            pb.inheritIO();
            try{
                final Process p = pb.start();
                p.waitFor();
            }
            catch (IOException | InterruptedException e) {
                throw Throwables.propagate(e);
            }

            Config storeParams = generateStoreParams(cluster, namespace);
            return TaskResult.defaultBuilder(request)
                .storeParams(storeParams)
                .build();
        }

        private void authCLI(Config commandRequestConfig) {
            String credentialJson = null;
            try {
                if (commandRequestConfig.has("credential_json")){
                    credentialJson = commandRequestConfig.get("credential_json", String.class).replaceAll("\n", "");
                }
                else if (commandRequestConfig.has("credential_json_path")){
                    String credentialPath = commandRequestConfig.get("credential_json_path", String.class);
                    credentialJson = new String(Files.readAllBytes(Paths.get(credentialPath))).replaceAll("\n", "");
                }
                else if (commandRequestConfig.has("credential_json_from_secret_key")){
                    String credentialSecretKey = commandRequestConfig.get("credential_json_from_secret_key", String.class);
                    credentialJson = context.getSecrets().getSecret(credentialSecretKey);
                }
            }
            catch (IOException e) {
                throw new ConfigException("Please check gcp credential file and file path.");
            }
            catch (SecretNotFoundException e) {
                throw new ConfigException(String.format("Could not access to secret:%s", commandRequestConfig.get("credential_json_from_secret_key", String.class)));
            }

            String authCommand = String.format("echo '%s' |  gcloud auth activate-service-account --key-file=-", credentialJson);
            List<String> authCommandList = Arrays.asList("/bin/bash", "-c", authCommand);
            ProcessBuilder pb = new ProcessBuilder(authCommandList);
            pb.inheritIO();
            try {
                final Process p = pb.start();
                p.waitFor();
            }
            catch (IOException | InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }

        @VisibleForTesting
        Config generateStoreParams(String cluster, String namespace){
            // set information for kubernetes command executor.
            String kubeConfigPath = System.getenv("KUBECONFIG");
            if (kubeConfigPath == null) {
                kubeConfigPath = Paths.get(System.getenv("HOME"), ".kube/config").toString();
            }

            io.fabric8.kubernetes.client.Config kubeConfig = getKubeConfigFromPath(kubeConfigPath);
            return cf.create().set("kubernetes", cf.create()
                .set("name", cluster)
                .set("master", kubeConfig.getMasterUrl())
                .set("certs_ca_data", kubeConfig.getCaCertData())
                .set("oauth_token", kubeConfig.getOauthToken())
                .set("namespace", namespace));
        }

        io.fabric8.kubernetes.client.Config getKubeConfigFromPath(String path)
        {
            try {
                final Path kubeConfigPath = Paths.get(path);
                final String kubeConfigContents = new String(Files.readAllBytes(kubeConfigPath), Charset.forName("UTF-8"));
                return io.fabric8.kubernetes.client.Config.fromKubeconfig(kubeConfigContents);
            } catch (java.io.IOException e) {
                throw new ConfigException("Could not read kubeConfig, check kube_config_path.");
            }
        }
    }
}
