package com.nutanix.prism.pcdr;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication(scanBasePackages = {"com.nutanix.prism.pcdr.*"})
public class ProtectionSpringBootApplication {
  public static void main(String[] args) throws InterruptedException {
    SpringApplication application =
        new SpringApplication(ProtectionSpringBootApplication.class);
    application.setBannerMode(Banner.Mode.OFF);
    log.info("Starting the Protection Service Spring Application");
    application.run(args);
  }
}