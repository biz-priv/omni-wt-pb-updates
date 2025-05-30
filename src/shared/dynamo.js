/*
 * File: src/shared/dynamo.js
 * Project: PB-WT 214
 * Author: Bizcloud Experts
 * Date: 2024-08-14
 * Confidential and Proprietary
 */

'use strict';

const AWS = require('aws-sdk');
const moment = require('moment-timezone');

const dynamoDB = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });
const _ = require('lodash');
const {
  types,
  status,
  milestones,
  getDynamoUpdateParam,
  checkAddressByGoogleApi,
} = require('./helper');

const {
  ORDERS_TABLE_NAME,
  MOVEMENT_ORDER_TABLE_NAME,
  ADD_MILESTONE_TABLE_NAME,
  ORDER_STATUS_TABLE_NAME,
  CONSOL_STATUS_TABLE_NAME,
  ADDRESS_MAPPING_TABLE,
  SHIPMENT_APAR_TABLE,
  SHIPMENT_APAR_INDEX_KEY_NAME,
  CONSOL_STOP_HEADERS,
  CONSOL_STOP_ITEMS,
  SHIPMENT_HEADER_TABLE,
  MOVEMENT_ORDER_INDEX_NAME,
  MOVEMENT_ORIGIN_INDEX_NAME,
  MOVEMENT_DESTINATION_INDEX_NAME,
  STOP_TABLE_NAME,
  STOP_INDEX_NAME,
  CONSOL_STOP_HEADERS_CONSOL_INDEX,
  STATUS_INDEX,
  CALLIN_TABLE,
  LOCATION_UPDATE_TABLE,
  FINALISE_COST_STATUS_TABLE,
  PB_USERS_TABLE,
  WT_USERS_TABLE,
  PB_CHARGES_TABLE,
  CONSIGNEE_TABLE,
  CONFIRMATION_COST,
  CONFIRMATION_COST_INDEX_KEY_NAME,
  SHIPMENT_APAR_REF_INDEX_KEY_NAME,
} = process.env;

async function query(params) {
  async function helper(params1) {
    try {
      const result = await dynamoDB.query(params1).promise();
      let data = _.get(result, 'Items', []);

      if (result.LastEvaluatedKey) {
        params1.ExclusiveStartKey = result.LastEvaluatedKey;
        data = data.concat(await helper(params1));
      }

      return data;
    } catch (error) {
      console.error('Query Item Error:', error, '\nQuery Params1:', params1);
      throw error;
    }
  }

  return helper(params);
}

async function getMovementOrder(id) {
  const movementParams = {
    TableName: MOVEMENT_ORDER_TABLE_NAME,
    IndexName: MOVEMENT_ORDER_INDEX_NAME,
    KeyConditionExpression: 'movement_id = :movement_id',
    ExpressionAttributeValues: {
      ':movement_id': id,
    },
    ProjectionExpression: 'order_id',
  };
  console.info('🙂 -> file: dynamo.js:59 -> getMovementOrder -> movementParams:', movementParams);

  try {
    const items = await query(movementParams);

    if (items.length > 0) {
      const orderId = _.get(items, '[0]order_id');
      console.info('Order ID:', orderId);
      return orderId;
    }
    return false;
  } catch (error) {
    console.error('Error in getMovementOrder function:', error);
    throw error;
  }
}

async function getOrder(orderId) {
  const orderParams = {
    TableName: ORDERS_TABLE_NAME,
    KeyConditionExpression: 'id = :id',
    ExpressionAttributeValues: {
      ':id': orderId,
    },
    ProjectionExpression: 'blnum',
  };
  console.info('🙂 -> file: dynamo.js:82 -> getOrder -> orderParams:', orderParams);

  try {
    const items = await query(orderParams);

    if (items.length > 0) {
      const housebillNum = _.get(items, '[0]blnum');
      console.info('Housebill Number:', housebillNum);
      return housebillNum;
    }
    throw new Error(`No Record found in Orders Dynamo Table for orderId: ${orderId}}`);
  } catch (error) {
    console.error('Error in getOrder function:', error);
    throw error;
  }
}

async function updateMilestone(finalPayload) {
  const addMilestoneParams = {
    TableName: ADD_MILESTONE_TABLE_NAME,
    Item: finalPayload,
  };
  console.info(
    '🙂 -> file: dynamo.js:110 -> updateMilestone -> addMilestoneParams:',
    addMilestoneParams
  );
  try {
    await dynamoDB.put(addMilestoneParams).promise();
    console.info('Successfully inserted payload into omni-pb-214-add-milestone table');
  } catch (error) {
    console.error('Error while updating data into 214-add-milestone Dynamo Table', error);
    throw error;
  }
}

async function getMovement(id, stopType) {
  const keyCondition = stopType === 'PU' ? 'origin_stop_id = :id' : 'dest_stop_id = :id';
  const indexName =
    stopType === 'PU' ? MOVEMENT_ORIGIN_INDEX_NAME : MOVEMENT_DESTINATION_INDEX_NAME;
  const movementParams = {
    TableName: process.env.MOVEMENT_DB,
    IndexName: indexName,
    KeyConditionExpression: keyCondition,
    FilterExpression: '#status IN (:statusP, :statusC)',
    ExpressionAttributeNames: {
      '#status': 'status',
    },
    ExpressionAttributeValues: {
      ':id': id,
      ':statusP': 'P',
      ':statusC': 'C',
    },
    ProjectionExpression: 'id',
  };
  console.info('🙂 -> file: dynamo.js:118 -> getMovement -> movementParams:', movementParams);

  try {
    const result = await query(movementParams);

    if (result.length > 0) {
      return _.get(result, '[0].id', '');
    }
    const movementParamsSecond = {
      TableName: process.env.MOVEMENT_DB,
      IndexName: indexName,
      KeyConditionExpression: keyCondition,
      FilterExpression: '#status IN (:statusD)',
      ExpressionAttributeNames: {
        '#status': 'status',
      },
      ExpressionAttributeValues: {
        ':id': id,
        ':statusD': 'D',
      },
      ProjectionExpression: 'id, inserted_timestamp',
    };
    console.info(
      '🙂 -> file: dynamo.js:164 -> getMovement -> movementParamsSecond:',
      movementParamsSecond
    );
    const resultSecond = await query(movementParamsSecond);
    console.info('🙂 -> file: dynamo.js:166 -> getMovement -> resultSecond:', resultSecond);
    const insertedTimestamp = _.get(
      resultSecond,
      '[0]inserted_timestamp',
      moment().tz('America/Chicago')
    );
    console.info(
      '🙂 -> file: dynamo.js:168 -> getMovement -> insertedTimestamp:',
      insertedTimestamp
    );
    const currentTimestamp = moment().tz('America/Chicago');
    const timeDifference = currentTimestamp.diff(insertedTimestamp, 'minutes');
    console.info('🙂 -> file: dynamo.js:171 -> getMovement -> timeDifference:', timeDifference);
    if (timeDifference < 5) {
      return _.get(resultSecond, '[0].id', '');
    }

    throw new Error(
      `No Record found in Movement Table for id: ${id}, index name: ${indexName} where status id 'P' or 'C'.\n
      The milestone cannot be updated.`
    );
  } catch (error) {
    console.error('Error in getMovement function:', error);
    throw error;
  }
}

async function getOrderStatus(id) {
  const orderStatusParams = {
    TableName: ORDER_STATUS_TABLE_NAME,
    IndexName: 'ShipmentId-index',
    KeyConditionExpression: 'ShipmentId = :ShipmentId',
    ExpressionAttributeNames: {
      '#type': 'Type',
    },
    ExpressionAttributeValues: {
      ':ShipmentId': id,
    },
    ProjectionExpression: '#type, FK_OrderNo, Housebill',
  };
  console.info(
    '🙂 -> file: dynamo.js:160 -> getOrderStatus -> orderStatusParams:',
    orderStatusParams
  );

  try {
    const items = await query(orderStatusParams);
    console.info('🙂 -> file: dynamo.js:224 -> getOrderStatus -> items:', items);

    if (items.length > 0) {
      return items[0];
    }
    console.info('No Record found in Order Status Dynamo Table for ShipmentId:', id);
    return false;
  } catch (error) {
    console.error('Error in getOrderStatus function:', error);
    throw error;
  }
}

async function getConsolStatus(id) {
  const consolStatusParams = {
    TableName: CONSOL_STATUS_TABLE_NAME,
    IndexName: 'ShipmentId-index',
    KeyConditionExpression: 'ShipmentId = :ShipmentId',
    ExpressionAttributeNames: {
      '#ConsolNo': 'ConsolNo',
      '#Type': 'Type',
    },
    ExpressionAttributeValues: {
      ':ShipmentId': id,
    },
    ProjectionExpression: '#ConsolNo, #Type, Housebill',
  };
  console.info(
    '🙂 -> file: dynamo.js:185 -> getConsolStatus -> consolStatusParams:',
    consolStatusParams
  );

  try {
    const items = await query(consolStatusParams);
    console.info('🙂 -> file: dynamo.js:258 -> getConsolStatus -> items:', items);

    if (items.length > 0) {
      return items[0];
    }
    console.info('No Record found in Consol Status Dynamo Table for ShipmentId:', id);
    return false;
  } catch (error) {
    console.error('Error in getMovementOrder function:', error);
    throw error;
  }
}

async function getStop(orderId) {
  const consolStatusParams = {
    TableName: STOP_TABLE_NAME,
    IndexName: STOP_INDEX_NAME,
    KeyConditionExpression: 'order_id = :order_id',
    ExpressionAttributeValues: {
      ':order_id': orderId,
    },
    ProjectionExpression: 'id',
  };
  console.info('🙂 -> file: dynamo.js:220 -> getStop -> consolStatusParams:', consolStatusParams);
  try {
    const items = await query(consolStatusParams);

    if (items.length > 0) {
      return items;
    }
    console.info('No Records found in Stop Dynamo Table for Order_Id:', orderId);
  } catch (error) {
    console.error('Error in Stop function:', error);
    throw error;
  }
  return true;
}

async function queryWithPartitionKey(tableName, key) {
  let params;
  try {
    const [expression, expressionAtts] = await getQueryExpression(key);
    params = {
      TableName: tableName,
      KeyConditionExpression: expression,
      ExpressionAttributeValues: expressionAtts,
    };
    return await dynamoDB.query(params).promise();
  } catch (error) {
    console.error('Query Item With Partition key Error: ', error, '\nGet params: ', params);
    throw new Error('QueryItemError');
  }
}

async function fetchConsignee(fkOrderNo) {
  const consignee = await queryWithPartitionKey(CONSIGNEE_TABLE, {
    FK_ConOrderNo: fkOrderNo,
  });
  return _.get(consignee, 'Items[0]');
}

async function fetchConfirmationCost(fkOrderNo, seqNo) {
  const confCostParam = {
    TableName: CONFIRMATION_COST,
    IndexName: CONFIRMATION_COST_INDEX_KEY_NAME,
    KeyConditionExpression: 'FK_OrderNo = :FK_OrderNo',
    FilterExpression: 'FK_SeqNo = :FK_SeqNo',
    ExpressionAttributeValues: {
      ':FK_OrderNo': fkOrderNo,
      ':FK_SeqNo': seqNo,
    },
  };
  const items = await dynamoDB.query(confCostParam).promise();
  items.Items.sort(
    (a, b) => new Date(b.InsertedTimeStamp) - new Date(a.InsertedTimeStamp) // Descending order
  );
  return _.get(items, 'Items[0]');
}

// Checking the consignee and confirmation cost address as the existing process is not giving accurate data
async function consigneeIsCustomerConsol(fkOrderNo, seqNo) {
  const consignee = await fetchConsignee(fkOrderNo);
  console.info('🙂 -> file: dynamo.js:346 -> consigneeIsCustomerConsol -> consignee:', consignee);
  const confirmationCost = await fetchConfirmationCost(fkOrderNo, seqNo);
  console.info(
    '🙂 -> file: dynamo.js:348 -> consigneeIsCustomerConsol -> confirmationCost:',
    confirmationCost
  );
  if (!consignee || !confirmationCost) {
    return false;
  }

  // The below code is copied from src/shared/addressMapping.js of omni-real-time-updates repo
  const payload = {
    cc_con_zip: '0',
    cc_con_address: '0',
    cc_conname: '0',
    cc_con_google_match: '0',
  };

  if (Object.hasOwn(consignee, 'ConZip') && Object.hasOwn(confirmationCost, 'ConZip')) {
    /**
     * HS or TL
     * cc_con_zip
     */
    if (consignee.ConZip === confirmationCost.ConZip) {
      payload.cc_con_zip = '1';

      /**
       * HS or TL
       * cc_con_address
       */
      if (
        consignee.ConAddress1 === confirmationCost.ConAddress1 &&
        consignee.ConAddress2 === confirmationCost.ConAddress2 &&
        consignee.ConCity === confirmationCost.ConCity &&
        consignee.FK_ConState === confirmationCost.FK_ConState &&
        consignee.FK_ConCountry === confirmationCost.FK_ConCountry
      ) {
        payload.cc_con_address = '1';
      } else {
        // check if we have data on shipmentApar table with  IVIA vendor T19262
        const address1 = `${consignee.ConAddress1}, ${consignee.ConAddress2}, ${consignee.ConCity}, ${consignee.FK_ConState}, ${consignee.FK_ConCountry}, ${consignee.ConZip}`;
        const address2 = `${confirmationCost.ConAddress1}, ${confirmationCost.ConAddress2}, ${confirmationCost.ConCity}, ${confirmationCost.FK_ConState}, ${confirmationCost.FK_ConCountry}, ${confirmationCost.ConZip}`;
        const { checkWithGapi, partialCheckWithGapi } = await checkAddressByGoogleApi(
          address1,
          address2
        );
        if (checkWithGapi) {
          payload.cc_con_google_match = '1';
        } else if (partialCheckWithGapi) {
          payload.cc_con_google_match = '2';
        }
      }
    }
  }
  console.info('🙂 -> file: dynamo.js:359 -> consigneeIsCustomerConsol -> payload:', payload);
  return !!(
    payload.cc_con_zip === '1' &&
    (payload.cc_con_address === '1' || payload.cc_con_google_match === '1')
  );
}

async function consigneeIsCustomer(fkOrderNo, type, seqNo) {
  let check = false;
  if (type === types.CONSOL) {
    return await consigneeIsCustomerConsol(fkOrderNo, seqNo);
  }

  // Query the address mapping table
  let addressMapRes = await queryWithPartitionKey(ADDRESS_MAPPING_TABLE, {
    FK_OrderNo: fkOrderNo,
  });

  console.info('addressMapRes', addressMapRes);
  if (addressMapRes.Items.length === 0) {
    console.info('Data not found on address mapping table for FK_OrderNo', fkOrderNo);
    return false; // No data found, return false
  }
  addressMapRes = addressMapRes.Items[0];

  if (type === types.MULTISTOP) {
    check = !!(
      addressMapRes.csh_con_zip === '1' &&
      (addressMapRes.csh_con_address === '1' || addressMapRes.csh_con_google_match === '1')
    );
  }
  return check;
}

async function getQueryExpression(keys) {
  let expression = '';
  const expressionAtts = {};
  Object.keys(keys).forEach((k) => {
    expression += `${k}=:${k} and `;
    expressionAtts[`:${k}`] = keys[k];
  });
  expression = expression.substring(0, expression.lastIndexOf(' and '));
  return [expression, expressionAtts];
}

async function getShipmentDetails({ shipmentId }) {
  const orderStatusRes = await getOrderStatus(shipmentId);
  if (orderStatusRes && _.get(orderStatusRes, 'Type') === types.NON_CONSOL) {
    return { ...orderStatusRes, housebill: _.get(orderStatusRes, 'Housebill') };
  }

  if (orderStatusRes && _.get(orderStatusRes, 'Type') === types.CONSOL) {
    return { ...orderStatusRes, housebill: _.get(orderStatusRes, 'FK_OrderNo') };
  }

  const consolStatusRes = await getConsolStatus(shipmentId);
  if (consolStatusRes) {
    return {
      ...consolStatusRes,
      Type: types.MULTISTOP,
      housebill: _.get(consolStatusRes, 'ConsolNo'),
    };
  }
  return {};
}

async function getShipmentDetailsapar({ shipmentId }) {
  try {
    const params = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName:  SHIPMENT_APAR_REF_INDEX_KEY_NAME, //'RefNo-FK_OrderNo-index',
      KeyConditionExpression: 'RefNo = :refno',
      FilterExpression: 'FK_VendorId = :vendor',
      ExpressionAttributeValues: {
        ':refno': String(shipmentId),
        ':vendor': 'LIVELOGI',
      },
      ProjectionExpression: 'FK_OrderNo, SeqNo, FK_ServiceId, ConsolNo, Consolidation, FK_VendorId',
    };
    console.info('🙂 -> file: dynamo.js:426 -> getShipmentDetailsapar -> params:', params);

    const result = await query(params);
    console.info('🙂 -> file: dynamo.js:426 -> getShipmentDetailsapar -> result:', result);
    const items = result;
    console.info('🙂 -> file: dynamo.js:426 -> getShipmentDetailsapar -> items:', items);

    if (!items || items.length === 0) {
      return {}
    }

    let shipmentType = 'unknown';
    let consolNo = '';
    let orderNo = '';
    let housebill = '';

    if (items.length === 1) {
      const item = items[0];
      console.info('🙂 -> file: dynamo.js:426 -> getShipmentDetailsapar -> item:', item);
      const { ConsolNo, FK_ServiceId: serviceLevelId, FK_VendorId: vendorId } = item;
      console.info(
        '🙂 -> file: dynamo.js:426 -> getShipmentDetailsapar -> FK_ServiceId:',
        serviceLevelId
      );
      console.info('🙂 -> file: dynamo.js:426 -> getShipmentDetailsapar -> FK_VendorId:', vendorId);
      console.info('🙂 -> file: dynamo.js:426 -> getShipmentDetailsapar -> ConsolNo:', ConsolNo);

      if (
        Number(ConsolNo) === 0 &&
        ['HS', 'TL'].includes(serviceLevelId) &&
        vendorId === 'LIVELOGI'
      ) {
        shipmentType = types.NON_CONSOL;
        orderNo = item.FK_OrderNo;
        consolNo = item.ConsolNo;
        const housebillData = await getShipmentHeaderData({ orderNo });
        console.info(
          '🙂 -> file: dynamo.js:426 -> getShipmentDetailsapar -> housebillData:',
          housebillData
        );
        housebill = housebillData[0].Housebill;
      }
    } else {
      const consolidationRecord = items.find((item) => item.SeqNo === '9999');
      if (consolidationRecord) {
        const serviceLevelId = consolidationRecord.FK_ServiceId;
        consolNo = consolidationRecord.ConsolNo;

        if (['HS', 'TL'].includes(serviceLevelId)) {
          shipmentType = types.CONSOL;
        } else if (serviceLevelId === 'MT') {
          shipmentType = types.MULTISTOP;
        }

        const filteredItems = items.filter((item) => item.SeqNo !== '9999');
        const orderNos = [...new Set(filteredItems.map((item) => item.FK_OrderNo))];
        orderNo = orderNos.join(',');
        console.info('🙂 -> file: dynamo.js:546 -> getShipmentDetailsapar -> orderNo:', orderNo);

        const housebillsArray = await Promise.all(
          orderNos.map(async (order) => {
            const res = await getShipmentHeaderData({ orderNo: order });
            return res?.[0]?.Housebill || ''; // safe access
          })
        );

        housebill = housebillsArray.filter(Boolean).join(',');
      }
    }

    return {
      shipmentType,
      consolNo,
      orderNo,
      housebill,
    };
  } catch (error) {
    console.error('Error in getShipmentDetailsapar:', error);
    throw error;
  }
}

async function getAparDataByConsole({ orderNo }) {
  try {
    const shipmentAparParams = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      FilterExpression: 'Consolidation = :consolidation',
      ExpressionAttributeValues: {
        ':ConsolNo': String(orderNo),
        ':consolidation': 'N',
      },
      ProjectionExpression: 'FK_OrderNo, SeqNo',
    };
    const result = await query(shipmentAparParams);
    console.info('🙂 -> file: dynamo.js:313 -> getAparDataByConsole -> result:', result);
    return result;
  } catch (error) {
    console.error('🙂 -> file: helper.js:546 -> error:', error);
    throw error;
  }
}

async function getConsolStopHeader({ consolNo, stopSeq }) {
  try {
    const cshparams = {
      TableName: CONSOL_STOP_HEADERS,
      IndexName: CONSOL_STOP_HEADERS_CONSOL_INDEX,
      KeyConditionExpression: 'FK_ConsolNo = :ConsolNo',
      FilterExpression: 'ConsolStopNumber = :stopSeq',
      ExpressionAttributeValues: {
        ':ConsolNo': consolNo.toString(),
        ':stopSeq': (Number(stopSeq) - 1).toString(),
      },
      ProjectionExpression: 'PK_ConsolStopId',
    };
    console.info('🙂 -> file: dynamo.js:331 -> getConsolStopHeader -> cshparams:', cshparams);
    const result = await query(cshparams);
    return result;
  } catch (error) {
    console.error('🙂 -> file: dynamo.js:338 -> getConsolStopHeader -> error:', error);
    throw error;
  }
}

async function getTotalStop({ consolNo }) {
  try {
    const cshparams = {
      TableName: CONSOL_STOP_HEADERS,
      IndexName: CONSOL_STOP_HEADERS_CONSOL_INDEX,
      KeyConditionExpression: 'FK_ConsolNo = :ConsolNo',
      ExpressionAttributeValues: {
        ':ConsolNo': consolNo.toString(),
      },
      ProjectionExpression: 'PK_ConsolStopId',
    };
    console.info('🙂 -> file: dynamo.js:331 -> getConsolStopHeader -> cshparams:', cshparams);
    const result = await query(cshparams);
    return result;
  } catch (error) {
    console.error('🙂 -> file: dynamo.js:338 -> getConsolStopHeader -> error:', error);
    throw error;
  }
}

async function getShipmentForSeq({ stopId }) {
  try {
    const cstparams = {
      TableName: CONSOL_STOP_ITEMS,
      IndexName: 'FK_ConsolStopId-index', // TODO: Move the index name to ssm
      KeyConditionExpression: 'FK_ConsolStopId = :stopId',
      ExpressionAttributeValues: {
        ':stopId': stopId.toString(),
      },
      ProjectionExpression: 'FK_OrderNo',
    };
    console.info('🙂 -> file: dynamo.js:347 -> getShipmentForSeq -> cstparams:', cstparams);
    const result = await query(cstparams);
    return result;
  } catch (error) {
    console.error('🙂 -> file: dynamo.js:358 -> getShipmentForSeq -> error:', error);
    throw error;
  }
}

async function getShipmentHeaderData({ orderNo }) {
  try {
    const shipmentHeaderParams = {
      TableName: SHIPMENT_HEADER_TABLE,
      KeyConditionExpression: 'PK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
      ProjectionExpression: 'Housebill',
    };
    console.info(
      '🙂 -> file: dynamo.js:380 -> getShipmentHeaderData -> shipmentHeaderParams:',
      shipmentHeaderParams
    );
    const result = await query(shipmentHeaderParams);
    return result;
  } catch (error) {
    console.error('🙂 -> file: dynamo.js:358 -> getShipmentForSeq -> error:', error);
    throw error;
  }
}

async function getShipmentForStop({ consolNo, stopSeq }) {
  console.info('🙂 -> file: helper.js:4 -> getConsolStopHeader:', getConsolStopHeader);
  const cshRes = await getConsolStopHeader({ consolNo, stopSeq });
  const stopId = _.get(cshRes, '[0].PK_ConsolStopId');
  if (!stopId) return [];
  return await getShipmentForSeq({ stopId });
}

async function fetchPendingPOD() {
  const params = {
    TableName: ADD_MILESTONE_TABLE_NAME,
    IndexName: STATUS_INDEX,
    KeyConditionExpression: '#status = :status and StatusCode = :statusCode',
    ExpressionAttributeNames: {
      '#status': 'Status',
      '#type': 'Type',
    },
    ExpressionAttributeValues: {
      ':status': status.PENDING,
      ':statusCode': milestones.POD,
    },
    ProjectionExpression: 'OrderId, Housebill, StatusCode, RetryCount, #type, EventDateTime',
  };
  console.info('🙂 -> file: dynamo.js:414 -> fetchPendingPOD -> params:', params);
  return await query(params);
}

async function updateDynamoRow({ housebill, statusCode, data }) {
  try {
    const { ExpressionAttributeNames, ExpressionAttributeValues, UpdateExpression } =
      getDynamoUpdateParam(data);
    const params = {
      TableName: process.env.ADD_MILESTONE_TABLE_NAME,
      Key: {
        Housebill: housebill,
        StatusCode: statusCode,
      },
      UpdateExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    };
    console.info('🙂 -> file: dynamodb.js:159 -> updateDocStatusTableData -> params:', params);
    return await dynamoDB.update(params).promise();
  } catch (error) {
    console.error('🙂 -> file: index.js:53 -> updateRow -> error:', error);
    throw error;
  }
}

async function updateStatusTable(
  Housebill,
  StatusCode,
  apiStatus,
  Payload = '',
  Response = '',
  ErrorMessage = '',
  message = ''
) {
  try {
    const updateParam = {
      TableName: ADD_MILESTONE_TABLE_NAME,
      Key: {
        Housebill,
        StatusCode,
      },
      UpdateExpression:
        'set Payload = :payload, #Response = :response, #Status = :status, EventDateTime = :eventDateTime, ErrorMessage = :errorMessage, Message = :message',
      ExpressionAttributeNames: {
        '#Status': 'Status',
        '#Response': 'Response',
      },
      ExpressionAttributeValues: {
        ':payload': String(Payload),
        ':response': String(Response),
        ':status': apiStatus,
        ':eventDateTime': moment.tz('America/Chicago').format(),
        ':errorMessage': ErrorMessage,
        ':message': message,
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDB.update(updateParam).promise();
  } catch (error) {
    console.error('🙂 -> file: index.js:224 -> error:', error);
    throw error;
  }
}

async function insertOrUpdateLocationUpdateTable({ housebill, callinId, data }) {
  try {
    const { ExpressionAttributeNames, ExpressionAttributeValues, UpdateExpression } =
      getDynamoUpdateParam(data);
    const params = {
      TableName: process.env.LOCATION_UPDATE_TABLE,
      Key: {
        Housebill: housebill,
        CallinId: callinId,
      },
      UpdateExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    };
    console.info(
      '🙂 -> file: dynamo.js:576 -> insertOrUpdateLocationUpdateTable -> params:',
      params
    );
    return await dynamoDB.update(params).promise();
  } catch (error) {
    console.error('🙂 -> file: index.js:579 -> insertOrUpdateLocationUpdateTable -> error:', error);
    throw error;
  }
}

async function getCallinDetails({ callinId }) {
  const orderParams = {
    TableName: CALLIN_TABLE,
    KeyConditionExpression: '#id = :id',
    ExpressionAttributeNames: {
      '#id': 'id',
      '#order_id': 'order_id',
      '#city_name': 'city_name',
      '#state': 'state',
      '#current_stop_id': 'current_stop_id',
    },
    ExpressionAttributeValues: {
      ':id': callinId,
    },
    ProjectionExpression: '#id, #order_id, #city_name, #state, #current_stop_id',
  };
  console.info('🙂 -> file: dynamo.js:82 -> getOrder -> orderParams:', orderParams);

  try {
    const items = await query(orderParams);
    console.info('🙂 -> file: dynamo.js:606 -> getCallinDetails -> items:', items);
    return _.get(items, '[0]', {});
  } catch (error) {
    console.error('Error in getOrder function:', error);
    throw error;
  }
}

async function getLocationUpdateDetails({ housebill, callinId }) {
  const orderParams = {
    TableName: LOCATION_UPDATE_TABLE,
    KeyConditionExpression: 'Housebill = :Housebill and CallinId = :CallinId',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':Housebill': housebill,
      ':CallinId': callinId,
    },
    ProjectionExpression: '#Status',
  };
  console.info('🙂 -> file: dynamo.js:82 -> getOrder -> orderParams:', orderParams);

  try {
    const items = await query(orderParams);
    console.info('🙂 -> file: dynamo.js:606 -> getCallinDetails -> items:', items);
    return _.get(items, '[0]', {});
  } catch (error) {
    console.error('Error in getOrder function:', error);
    throw error;
  }
}

async function deleteDynamoRecord({ tableName, pKeyName, pKey, sKeyName, sKey }) {
  const param = {
    TableName: tableName,
    Key: {
      [pKeyName]: pKey,
      [sKeyName]: sKey,
    },
  };
  return await dynamoDB.delete(param).promise();
}

async function getStopDetails({ id }) {
  const stopParam = {
    TableName: STOP_TABLE_NAME,
    KeyConditionExpression: '#id = :id',
    ExpressionAttributeNames: { '#id': 'id' },
    ExpressionAttributeValues: {
      ':id': id,
    },
    ProjectionExpression: 'movement_sequence',
  };
  console.info('🙂 -> file: dynamo.js:659 -> getStopDetails -> stopParam:', stopParam);

  try {
    const items = await query(stopParam);
    console.info('🙂 -> file: dynamo.js:663 -> getStopDetails -> items:', items);
    return _.get(items, '[0]', {});
  } catch (error) {
    console.error('Error in getOrder function:', error);
    throw error;
  }
}

/**
 * Stores the finalize cost status in DynamoDB.
 * @param {Object} params - Parameters for storing in DynamoDB
 */
async function storeFinalizeCostStatus({
  shipmentInfo,
  shipmentId,
  finaliseCostRequest,
  response,
  Status,
  errorMessage = null,
  type,
}) {
  const { orderNo, consolNo, housebill } = shipmentInfo;
  // Construct the base item
  const item = {
    ShipmentId: shipmentId,
    Housebill: housebill.toString(),
    ConsolNo: consolNo,
    Payload: finaliseCostRequest,
    Response: response,
    Status,
    ErrorMsg: errorMessage,
    UpdatedAt: moment.tz('America/Chicago').format(),
    Type: type,
  };

  // Add ConsolNo only if type is not MULTISTOP
  if (type !== types.MULTISTOP) {
    item.OrderNo = orderNo;
  }

  const params = {
    TableName: FINALISE_COST_STATUS_TABLE,
    Item: item,
  };

  try {
    await dynamoDB.put(params).promise();
    console.info(`Successfully stored finalize cost status for shipment ${shipmentId}`);
  } catch (error) {
    console.error('Error storing finalize cost status in DynamoDB:', error);
    throw new Error('Failed to store finalize cost status object.');
  }
}

async function isAlreadyProcessed(shipmentId) {
  try {
    const params = {
      TableName: FINALISE_COST_STATUS_TABLE,
      KeyConditionExpression: 'ShipmentId = :shipmentId',
      ExpressionAttributeValues: {
        ':shipmentId': String(shipmentId),
      },
    };

    const result = await dynamoDB.query(params).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error(`Error checking ${FINALISE_COST_STATUS_TABLE} table:`, error);
    throw error;
  }
}

async function queryUsersTable({ userId }) {
  try {
    if (userId === 'NULL' || userId === 'NA' || !userId) {
      return '';
    }

    const params = {
      TableName: PB_USERS_TABLE,
      KeyConditionExpression: 'id = :userid',
      ExpressionAttributeValues: {
        ':userid': userId,
      },
      ProjectionExpression: 'email_address',
    };

    const result = await dynamoDB.query(params).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error(`Error checking ${PB_USERS_TABLE} table:`, error);
    throw error;
  }
}

async function fetchUserEmail({ userId }) {
  try {
    if (userId === 'NULL' || userId === 'na' || userId === '') {
      return '';
    }
    const params = {
      TableName: WT_USERS_TABLE,
      KeyConditionExpression: 'PK_UserId = :PK_UserId',
      ProjectionExpression: 'UserEmail',
      ExpressionAttributeValues: {
        ':PK_UserId': userId,
      },
    };
    console.info('🚀 ~ file: helper.js:759 ~ fetchUserEmail ~ param:', params);
    const response = await dynamoDB.query(params).promise();
    return _.get(response, 'Items[0].UserEmail', '');
  } catch (error) {
    console.error('🙂 -> file: helper.js:831 -> error:', error);
    throw error;
  }
}

async function queryChargesTable({ shipmentId }) {
  try {
    const params = {
      TableName: PB_CHARGES_TABLE,
      IndexName: 'order_id-index',
      KeyConditionExpression: 'order_id = :id',
      ProjectionExpression: 'order_id,amount,charge_id,descr',
      ExpressionAttributeValues: {
        ':id': shipmentId,
      },
    };
    console.info('🚀 ~ file: helper.js:841 ~ fetchUserEmail ~ param:', params);
    const response = await dynamoDB.query(params).promise();
    return _.get(response, 'Items', []);
  } catch (error) {
    console.error('🙂 -> file: helper.js:831 -> error:', error);
    throw error;
  }
}

async function queryShipmentAparTable(consolNo) {
  const params = {
    TableName: SHIPMENT_APAR_TABLE,
    IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
    KeyConditionExpression: 'ConsolNo = :ConsolNo',
    FilterExpression: 'Consolidation = :consolidation AND FK_VendorId = :vendor',
    ExpressionAttributeValues: {
      ':ConsolNo': consolNo,
      ':consolidation': 'N',
      ':vendor': 'LIVELOGI',
    },
    ProjectionExpression: 'FK_OrderNo',
  };

  try {
    const result = await dynamoDB.query(params).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying SHIPMENT_APAR_TABLE:', error);
    throw error;
  }
}

async function getStationCode(orderno, type, consolNo) {
  let params;
  let orderNo;
  if (type === types.NON_CONSOL) {
    orderNo = orderno;
  } else {
    orderNo = consolNo;
  }
  const aparParams = {
    TableName: SHIPMENT_APAR_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo and SeqNo = :seqNo',
    FilterExpression: 'FK_VendorId = :vendor',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
      ':vendor': 'LIVELOGI',
      ':seqNo': '9999',
    },
    ProjectionExpression: 'FK_OrderNo, FK_ConsolStationId',
  };
  const headerParams = {
    TableName: SHIPMENT_HEADER_TABLE,
    KeyConditionExpression: 'PK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
    ProjectionExpression: 'Housebill, ControllingStation',
  };
  if (type === types.NON_CONSOL) {
    params = headerParams;
  } else {
    params = aparParams;
  }

  try {
    const result = await dynamoDB.query(params).promise();
    const items = _.get(result, 'Items', []);
    let stationCode;
    if (type === types.NON_CONSOL) {
      stationCode = _.get(items, '[0].ControllingStation', null);
    } else {
      stationCode = _.get(items, '[0].FK_ConsolStationId', null);
    }

    if (!stationCode || stationCode === '' || stationCode === 'NULL') {
      throw new Error(`Invalid station code: ${stationCode}`);
    }
    return stationCode;
  } catch (error) {
    console.error('Error querying SHIPMENT_APAR_TABLE:', error);
    throw error;
  }
}

module.exports = {
  getMovementOrder,
  getOrder,
  updateMilestone,
  getMovement,
  getOrderStatus,
  getConsolStatus,
  getStop,
  queryWithPartitionKey,
  consigneeIsCustomer,
  getShipmentDetails,
  getShipmentDetailsapar,
  getAparDataByConsole,
  getShipmentHeaderData,
  getConsolStopHeader,
  getShipmentForSeq,
  getShipmentForStop,
  fetchPendingPOD,
  updateDynamoRow,
  getTotalStop,
  updateStatusTable,
  insertOrUpdateLocationUpdateTable,
  getCallinDetails,
  getLocationUpdateDetails,
  deleteDynamoRecord,
  getStopDetails,
  storeFinalizeCostStatus,
  isAlreadyProcessed,
  queryUsersTable,
  fetchUserEmail,
  queryChargesTable,
  queryShipmentAparTable,
  getStationCode,
};
