package com.nutanix.prism.pcdr.restserver.util;

/*
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: ruchil.prajapati@nutanix.com
 */


import com.nutanix.api.utils.links.ApiLinkUtils;
import com.nutanix.api.utils.links.PaginationLinkUtils;
import com.nutanix.api.utils.links.TaskLinkUtils;
import com.nutanix.prism.pcdr.restserver.constants.Constants;
import dp1.pri.common.v1.response.ApiLink;
import dp1.pri.prism.v4.config.TaskReference;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class to get ApiLink of task, pagination, self link.
 * This is used mainly when adding metadata in responses.
 */

public class ApiLinkUtil {

  private ApiLinkUtil() {}

  /**
   * Returns the current HttpServletRequest.
   * @return the current request.
   */


  static HttpServletRequest getRequest() {
    final ServletRequestAttributes attrs = (ServletRequestAttributes)
        RequestContextHolder.currentRequestAttributes();
    return attrs.getRequest();
  }

  /**
   * Returns the list of ApiLinks for pagination query.
   *
   * @param totalEntities task identifier.
   * @return Link to cancel the task.
   */
  public static List<ApiLink> paginationLinks(
      final Integer totalEntities, final Map<String, String> allQueryParams) {
    final HttpServletRequest request = getRequest();
    String completeUrl = request.getRequestURL().toString();
    StringBuilder queryString = new StringBuilder();
    boolean firstParam = true;

    for (Map.Entry<String, String> queryParam : allQueryParams.entrySet()) {
      if (firstParam) {
        queryString.append("?");
        firstParam = false;
      } else {
        queryString.append("&");
      }
      queryString.append(queryParam.getKey()).append("=").append(queryParam.getValue());
    }

    completeUrl += queryString.toString();

    final Map<String, String> paginationLinkMap =
        PaginationLinkUtils.getPaginationLinks(totalEntities, completeUrl);
    List<ApiLink> apiLinkList = new ArrayList<>();
    paginationLinkMap.forEach((rel, url) -> apiLinkList.add(new ApiLink(url, rel)));
    return apiLinkList;
  }

  /**
   * Returns the list of self link for a request.
   *
   * @return List of self ApiLink.
   */
  public static ApiLink getSelfLink() {
    final HttpServletRequest request = getRequest();
    ApiLink apiLink = new ApiLink();
    apiLink.setHref(ApiLinkUtils.getFullUri(request.getServletPath(), ApiLinkUtils.getOriginHostPort(request), ApiLinkUtils.getRequestFullVersion(request)));
    apiLink.setRel(Constants.SELF_LINK_RELATION);
    return apiLink;
  }

  /**
   * Returns the task link for a request.
   *
   * @param taskReference Task Reference of which ApiLink to return
   * @return task ApiLink.
   */
  public static ApiLink getApiLinkForTaskReference(final TaskReference taskReference) {
    ApiLink apiLink = new ApiLink();
    apiLink.setHref(TaskLinkUtils.getTaskUri(getRequest(), taskReference, taskReference.getExtId()));
    apiLink.setRel(Constants.TASK_LINK_RELATION);
    return apiLink;
  }
}
