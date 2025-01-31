package com.nutanix.prism.pcdr.restserver.config;

import com.nutanix.insights.insights_interface.InsightsInterface;
import com.nutanix.prism.base.zk.BasicConnectionManagingZkClient;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.proxy.GenesisRemoteProxyImpl;
import com.nutanix.prism.service.EntityDbServiceHelper;
import com.nutanix.prism.service.ErgonService;
import com.nutanix.prism.service.ErgonServiceImpl;
import com.nutanix.prism.util.common.CaCertificateChainUtil;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.LocaleResolver;
import org.springframework.web.servlet.i18n.AcceptHeaderLocaleResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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

  @Qualifier("PcdrRestTemplate")
  @Bean
  RestTemplate getRestTemplate() {
    return new RestTemplate();
  }

  @Bean
  CaCertificateChainUtil getCaCertificateChainUtil() { return new CaCertificateChainUtil(
      BasicConnectionManagingZkClient.getInstance()); }

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
}
