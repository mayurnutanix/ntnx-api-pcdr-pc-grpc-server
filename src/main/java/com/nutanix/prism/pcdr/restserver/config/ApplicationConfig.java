package com.nutanix.prism.pcdr.restserver.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.nutanix.insights.insights_interface.InsightsInterface;
import com.nutanix.prism.base.zk.BasicConnectionManagingZkClient;
import com.nutanix.prism.base.zk.ICloseNullEnabledZkConnection;
import com.nutanix.prism.base.zk.ZkInjector;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.proxy.GenesisRemoteProxyImpl;
import com.nutanix.prism.proxy.ProxyInjector;
import com.nutanix.prism.service.EntityDbServiceHelper;
import com.nutanix.prism.service.ErgonService;
import com.nutanix.prism.service.ErgonServiceImpl;
import com.nutanix.prism.util.common.CaCertificateChainUtil;
import com.nutanix.prism.util.idempotency.IdempotencySupportServiceImpl;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.LocaleResolver;
import org.springframework.web.servlet.i18n.AcceptHeaderLocaleResolver;
import proto3.dp1.pri.prism.v4.management.mappers.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;

@EnableRetry
@Configuration
@Getter
@Setter
@Slf4j
public class ApplicationConfig {

  @Value("${prism.pcdr.basePathForYamls}")
  private String basePathForYamls;

  @Value("${prism.pcdr.idf.ip:127.0.0.1}")
  private String idfIp;

  @Value("${prism.pcdr.genesis.ip:127.0.0.1}")
  private String genesisIp;

  @Value("${prism.pcdr.idf.port:2027}")
  private int idfPort;

  @Value("${prism.pcdr.supportedLocales:en-US}")
  private List<String> supportedLocales;

  @Value("${adonis.service.thread.pool:10}")
  private Integer adonisPoolSize;

  @Value("${adonis.service.scheduled.thread.pool:2}")
  private Integer adonisScheduledPoolSize;

  @Bean
  InsightsInterface getEntityDbService() {
    return new InsightsInterface(idfIp, idfPort);
  }

  @Bean
  EntityDbServiceHelper getEntityDbServiceHelper(){
    return new EntityDbServiceHelper();
  }

  @Bean(name = "pcdr.ergon")
  ErgonService getErgonService() {
    return ErgonServiceImpl.getInstance();
  }

  @Bean
  InstanceServiceFactory getInstanceServiceFactory() {
    return new InstanceServiceFactory();
  }

  @Bean
  GenesisRemoteProxyImpl getGenesisRemoteProxy() {
    return new GenesisRemoteProxyImpl(genesisIp);
  }

  @Qualifier("serviceTemplate")
  //@Lazy
  @Bean
  RestTemplate getRestTemplate() {
    return new RestTemplate();
  }

  @Bean
  public ProxyInjector proxyInjector(ConfigurableEnvironment configurableEnvironment) {
    return new ProxyInjector(configurableEnvironment);
  }

  @Bean
  public ZkInjector zkInjector(ProxyInjector proxyInjector) {
    return new ZkInjector(proxyInjector);
  }

  @Bean
  ICloseNullEnabledZkConnection getZkClient(ZkInjector zkInjector) {
    return BasicConnectionManagingZkClient.getInstance();
  }
  @Bean
  CaCertificateChainUtil getCaCertificateChainUtil(ZkInjector zkInjector) { return new CaCertificateChainUtil(
      BasicConnectionManagingZkClient.getInstance()); }

  @Bean
  BackupTargetMapper backupTargetMapper(){
    return new BackupTargetMapperImpl();
  }

  @Bean
  public CreateBackupTargetApiResponseMapper createBackupTargetApiResponseMapper(){
    return new CreateBackupTargetApiResponseMapperImpl();
  }

  @Bean
  public DeleteBackupTargetApiResponseMapper deleteBackupTargetApiResponseMapper(){
    return new DeleteBackupTargetApiResponseMapperImpl();
  }

  @Bean
  public GetBackupTargetApiResponseMapper getBackupTargetApiResponseMapper(){
    return new GetBackupTargetApiResponseMapperImpl();
  }

  @Bean
  public ListBackupTargetsApiResponseMapper listBackupTargetsApiResponseMapper(){
    return new ListBackupTargetsApiResponseMapperImpl();
  }

  @Bean
  public UpdateBackupTargetApiResponseMapper updateBackupTargetApiResponseMapper(){
    return new UpdateBackupTargetApiResponseMapperImpl();
  }

  @Bean
  public ObjectMapper objectMapper(){
    return new ObjectMapper()
            .configure(DEFAULT_VIEW_INCLUSION, false)
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }


  @Bean
  public ExecutorService adonisServiceThreadPool() {
    return Executors.newFixedThreadPool(adonisPoolSize);
  }

  @Bean
  public ScheduledExecutorService adonisServiceScheduledThreadPool() {
    return Executors.newScheduledThreadPool(adonisScheduledPoolSize);
  }

  @Bean
  public LocaleResolver localeResolver() {
    AcceptHeaderLocaleResolver resolver = new AcceptHeaderLocaleResolver();
    resolver.setDefaultLocale(Locale.US);
    ArrayList<Locale> supportedLocaleLangTag = new ArrayList<>();
    for(String locale: supportedLocales){
      supportedLocaleLangTag.add(Locale.forLanguageTag(locale));
    }
    resolver.setSupportedLocales(supportedLocaleLangTag);
    return resolver;
  }

  @Bean
  public RateLimiterRegistry rateLimiterRegistry() {
    return RateLimiterRegistry.ofDefaults();
  }

  @Bean
  IdempotencySupportServiceImpl getIdempotencySupportService() {
    return new IdempotencySupportServiceImpl();
  }
}
