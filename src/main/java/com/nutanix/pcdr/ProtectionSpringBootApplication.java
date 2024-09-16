package com.nutanix.pcdr;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.nutanix.pcdr.*"})
public class ProtectionSpringBootApplication {
  public static void main(String[] args) {
    SpringApplication application =
        new SpringApplication(ProtectionSpringBootApplication.class);
    application.setBannerMode(Banner.Mode.OFF);
    System.out.println("mayur we are here");
    application.run(args);
  }
}

