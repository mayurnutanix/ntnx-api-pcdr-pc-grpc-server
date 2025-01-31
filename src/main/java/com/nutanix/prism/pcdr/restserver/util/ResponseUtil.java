/*
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: ruchil.prajapati@nutanix.com
 */

package com.nutanix.prism.pcdr.restserver.util;

import dp1.pri.common.v1.config.Flag;
import dp1.pri.common.v1.response.ApiLink;
import dp1.pri.common.v1.response.ApiResponseMetadata;

import java.util.List;
import java.util.Map;

import static com.nutanix.prism.pcdr.restserver.constants.Constants.HAS_ERROR;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.IS_PAGINATED;

/**
 * Utility class to get ApiResponseMetadata
 * for multiple scenarios like pagination, error processing request.
 */

public class ResponseUtil {


  /**
   * Create a new ApiResponseMetadata for List API response.
   * @param totalEntityCount Total number of entities present in the DB.
   * @param allQueryParams Map of query parameters.
   * @return a new ApiResponseMetadata with the total entity count, pagination flag and pagination links.
   */
  public static ApiResponseMetadata createListMetadata(final Integer totalEntityCount,
                                                       final Map<String, String> allQueryParams) {
    ApiResponseMetadata metadata = new ApiResponseMetadata();

    if (totalEntityCount != null) {
      // Set the Total Number of Available Entities
      metadata.setTotalAvailableResults(totalEntityCount);

      // Add all pagination links
      if(allQueryParams.containsKey("$limit") && allQueryParams.containsKey("$page")) {
        metadata.setLinks(ApiLinkUtil.paginationLinks(totalEntityCount, allQueryParams));
      }

      // Set the isPaginated Flag
      List<Flag> flags = metadata.getFlags();

      flags.stream().filter(flag -> IS_PAGINATED.equals(flag.getName()))
          .forEach(flag -> flag.setValue(true));

      metadata.setFlags(flags);
    }

    return metadata;
  }

  /**
   * Create a new ApiResponseMetadata for general response.
   * @param apiLinks List of ApiLink
   * @param hasError boolean marking if error processing request
   * @param isPaginated boolean marking if response is paginated
   * @return a new ApiResponseMetadata with the total entity count, pagination flag and pagination links.
   */
  public static ApiResponseMetadata createMetadataFor(
      final List<ApiLink> apiLinks, final boolean hasError, final boolean isPaginated) {

    ApiResponseMetadata metadata = new ApiResponseMetadata();
    metadata.setLinks(apiLinks);
    List<Flag> flags = metadata.getFlags();
    flags.forEach(flag -> {
      if (HAS_ERROR.equals(flag.getName())) {
        flag.setValue(hasError);
      }
      if (IS_PAGINATED.equals(flag.getName())) {
        flag.setValue(isPaginated);
      }
    });
    metadata.setFlags(flags);

    return metadata;
  }

  /**
   * Create a new ApiResponseMetadata for general response.
   * @param hasError boolean marking if error processing request
   * @return a new ApiResponseMetadata with hasError flag.
   */
  public static ApiResponseMetadata createMetadataFor(
      final boolean hasError) {
    ApiResponseMetadata metadata = new ApiResponseMetadata();
    List<Flag> flags = metadata.getFlags();
    flags.stream().filter(flag -> HAS_ERROR.equals(flag.getName()))
        .forEach(flag -> flag.setValue(hasError));
    metadata.setFlags(flags);
    return metadata;
  }

}
