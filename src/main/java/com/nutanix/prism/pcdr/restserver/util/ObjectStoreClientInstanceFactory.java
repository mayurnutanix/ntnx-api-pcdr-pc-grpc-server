package com.nutanix.prism.pcdr.restserver.util;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class ObjectStoreClientInstanceFactory {
  /**
   * Function that creates a S3 client.
   *
   * @param
   * @return AmazonS3 - S3 client
   */
  public static AmazonS3 getS3Client(Regions regions) {
    //set-up the client
    AmazonS3 s3client = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .withRegion(regions)
        .build();
    return s3client;
  }
}