package com.nutanix.prism.pcdr.restserver.util;

/**
 * Copyright (c) 2023 Nutanix Inc. All rights reserved.
 *
 * Author: mayur.ramavat@nutanix.com
 */

import com.google.common.collect.Lists;
import com.nutanix.prism.dto.groups.GroupQueryDTO;
import com.nutanix.prism.dto.groups.RequestAttributeDTO;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.HttpHelper;
import com.nutanix.prism.pcdr.util.RequestUtil;
import com.nutanix.prism.pojos.GroupEntityResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.*;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.VIRTUAL_NIC_TABLE_GROUPS;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.VM_TABLE_GROUPS;


@Slf4j
@Service
public class GroupsUtil {

  @Autowired
  private HttpHelper httpHelper;
  private static final String GROUPS_FILTER_IN_DELIMITER = "|";
  private static final String GROUPS_FILTER_IN_MARKER = "=in=";

  /**
   * Fetch pcvm specs from hosting PE using v3/Groups api call .
   *
   * @param ip         Hosting PE ip.
   * @param vmUuidList vmuuid of PC's.
   * @return GroupEntityResult object containing pcvm specs info.
   * @throws PCResilienceException
   */
  public ResponseEntity<GroupEntityResult> fetchPCVmSpecs(String ip, List<String> vmUuidList) throws PCResilienceException {
    try {
      GroupQueryDTO groupQueryDTO = createPayloadForVmGroupsApi(vmUuidList);
      ResponseEntity<GroupEntityResult> response = postGroupsCall(groupQueryDTO, ip);
      return response;
    }
    catch (Exception e) {
      log.error("Exception occurred while fetching vm details from groups api", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      throw new PCResilienceException(exceptionDetails.getMessage(),exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }
  }

  /**
   * Fetch virtual network specs from hosting PE using v3/Groups api call .
   *
   * @param ip         Hosting PE ip.
   * @param vmUuidList vmuuid of PC's.
   * @return GroupEntityResult object containing pcvm specs info.
   * @throws PCResilienceException
   */
  public ResponseEntity<GroupEntityResult> fetchVirtualNetworkSpecsFromHostingPE(String ip, List<String> vmUuidList) throws PCResilienceException {
    try {
      GroupQueryDTO groupQueryDTO = createPayloadForVirtualNicGroupsApi(vmUuidList);
      ResponseEntity<GroupEntityResult> response = postGroupsCall(groupQueryDTO,ip);
      return response;
    }
    catch (Exception e) {
      log.error("Exception occurred while fetching virtual nic details from groups api.", e);
      throw new PCResilienceException(ErrorMessages.FETCH_NIC_DETAILS_ERROR,
              ErrorCode.PCBR_FETCH_PC_VNIC_FAILURE,HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }


   /**
   * Call v3/groups api using http rest client .
   * @param groupQueryDTO   query requestbody.
   * @param ip         Hosting PE ip.
   * @return GroupEntityResult object containing groups response info.
   * @throws PCResilienceException
   */
  private ResponseEntity<GroupEntityResult> postGroupsCall(GroupQueryDTO groupQueryDTO, String ip) throws PCResilienceException {
         HttpHeaders serviceAuthHeaders = httpHelper.getServiceAuthHeaders(Constants.PRISM_SERVICE, Constants.PRISM_KEY_PATH,
                                                            Constants.PRISM_CERT_PATH);
      serviceAuthHeaders.setContentType(MediaType.APPLICATION_JSON);
      HttpEntity<?> requestEntity = new HttpEntity<>(groupQueryDTO, serviceAuthHeaders);
      String completeURL = RequestUtil.makeFullUrl(ip, V3_GROUPS_API_URL, JGW_PORT,
          Collections.emptyMap(), Collections.emptyMap(), true);
      ResponseEntity<GroupEntityResult> response = httpHelper.invokeAPI(completeURL, requestEntity,
          HttpMethod.POST, GroupEntityResult.class);
      validateGroupsResponse(response);
      return response;
  }


  private GroupQueryDTO createPayloadForVmGroupsApi(List<String> vmUuidList) {
    GroupQueryDTO groupQueryDTO = new GroupQueryDTO();
    groupQueryDTO.setEntityType(VM_TABLE_GROUPS);
    groupQueryDTO.setQueryName("prism_pcdr:vm");
    final List<String> requestAttributes = Lists.newArrayList(NUM_VCPUS, MEMORY_SIZE_BYTES, CONTAINER_UUIDS, VM_NAME);
    // set vmUuidList as entityIds as it is primary key of entity table
    groupQueryDTO.setEntityIDs(vmUuidList);
    groupQueryDTO.setGroupMemberAttributes(requestAttributes.stream()
                                                            .map(requestAttribute -> new RequestAttributeDTO(
                                                                requestAttribute, null))
                                                            .collect(Collectors.toList()));
    return groupQueryDTO;
  }

  public GroupQueryDTO createPayloadForVirtualNicGroupsApi(List<String> virtualNicIdList) {
    GroupQueryDTO groupQueryDTO = new GroupQueryDTO();
    groupQueryDTO.setEntityType(VIRTUAL_NIC_TABLE_GROUPS);
    groupQueryDTO.setQueryName("prism_pcdr:virtual_nic");
    final List<String> requestAttributes = Lists.newArrayList(VIRTUAL_NETWORK, VM_ATTRIBUTE);
    groupQueryDTO.setGroupMemberAttributes(requestAttributes.stream()
                                                            .map(requestAttribute -> new RequestAttributeDTO(
                                                                requestAttribute, null))
                                                            .collect(Collectors.toList()));
    // Setting VMuuid list as filter for virtual_nic table
    String filterAttribute = String.join(GROUPS_FILTER_IN_DELIMITER, virtualNicIdList);
    StringBuilder filterAttributeBuilder = new StringBuilder();
    filterAttributeBuilder.append(VM_ATTRIBUTE)
                          .append(GROUPS_FILTER_IN_MARKER)
                          .append(filterAttribute);
    groupQueryDTO.setFilterCriteria(filterAttributeBuilder.toString());
    return groupQueryDTO;
  }


  /**
   * Return data value for attributes in groups response
   * Sample :- "data":[{"values":[{"values":["4e69af0a-9a58-4e4e-a437-369a97e89930"],"time":1702478554715433}],"name":"vm"}
   *
   * @param param attribute key :- "vm" in sample.
   * @return null or List<String> value for attribute.
   * @throws PCResilienceException
   */
  public List<String> getParameterValueFromGroups(String param,
                                                  List<GroupEntityResult.GroupResult.EntityResult.MetricData> dataList) {

    for (GroupEntityResult.GroupResult.EntityResult.MetricData data : dataList) {
      if (data.getName().equals(param) && !CollectionUtils.isEmpty(data.getValues()) &&
          !CollectionUtils.isEmpty(data.getValues().get(0).getValues())) {
        return data.getValues().get(0).getValues();
      }
    }
    return null;
  }

  private void validateGroupsResponse(ResponseEntity<GroupEntityResult> response) throws PCResilienceException {
    if (!response.hasBody() || response.getBody().getGroupResults().isEmpty() ||
        response.getBody().getGroupResults().get(0).getEntityResults().isEmpty() ||
        response.getBody().getGroupResults().get(0).getEntityResults().get(0).getData().isEmpty()) {
      log.debug("Groups Api response : {}", response);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
  }
}



