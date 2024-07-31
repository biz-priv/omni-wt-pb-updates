'use strict';

const AWS = require('aws-sdk');

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const _ = require('lodash');
const { types } = require('./helper');

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
    IndexName: MOVEMENT_ORDER_INDEX_NAME, // TODO: Move the index name to ssm
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
    throw new Error('No Record found in Movement order Dynamo Table for Id:', id);
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
    throw new Error('No Record found in Orders Dynamo Table for orderId:', orderId);
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
  try {
    await dynamoDB.put(addMilestoneParams).promise();
    console.info('Successfully inserted payload into omni-pb-214-add-milestone table');
  } catch (error) {
    console.error('Error while updating data into 214-add-milestone Dynamo Table', error);
    throw error;
  }
}

async function getMovement(id, stopType) {
  const movementParams = {
    TableName: process.env.MOVEMENT_DB,
    IndexName: stopType === 'PU' ? MOVEMENT_ORIGIN_INDEX_NAME : MOVEMENT_DESTINATION_INDEX_NAME, // TODO: Move the index name to ssm
    KeyConditionExpression: stopType === 'PU' ? 'origin_stop_id = :id' : 'dest_stop_id = :id',
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
    throw new Error('No Record found in Movement Table for id: ', id);
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
    ProjectionExpression: '#type, FK_OrderNo',
  };
  console.info(
    '🙂 -> file: dynamo.js:160 -> getOrderStatus -> orderStatusParams:',
    orderStatusParams
  );

  try {
    const items = await query(orderStatusParams);

    if (items.length > 0) {
      return items[0];
    }
    console.info('No Record found in Order Status Dynamo Table for MovementId:', id);
    return false;
  } catch (error) {
    console.error('Error in getOrderStatus function:', error);
    throw error;
  }
}

async function getConsolStatus(id) {
  const consolStatusParams = {
    TableName: CONSOL_STATUS_TABLE_NAME,
    IndexName: 'ShipmentId-index', // TODO: Move the index name to ssm
    KeyConditionExpression: 'ShipmentId = :ShipmentId',
    ExpressionAttributeNames: {
      '#ConsolNo': 'ConsolNo',
      '#Type': 'Type',
    },
    ExpressionAttributeValues: {
      ':ShipmentId': id,
    },
    ProjectionExpression: '#ConsolNo, #Type',
  };
  console.info(
    '🙂 -> file: dynamo.js:185 -> getConsolStatus -> consolStatusParams:',
    consolStatusParams
  );

  try {
    const items = await query(consolStatusParams);

    if (items.length > 0) {
      return items[0];
    }
    console.info('No Record found in Consol Status Dynamo Table for MovementId:', id);
    return false;
  } catch (error) {
    console.error('Error in getMovementOrder function:', error);
    throw error;
  }
}

async function getStop(orderId) {
  const consolStatusParams = {
    TableName: STOP_TABLE_NAME, // TODO: Move the table name to ssm
    IndexName: STOP_INDEX_NAME, // TODO: Move the index name to ssm
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
  } catch (e) {
    console.error('Query Item With Partition key Error: ', e, '\nGet params: ', params);
    throw new Error('QueryItemError');
  }
}

async function consigneeIsCustomer(fkOrderNo, type) {
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

  let check = false;
  if (type === types.CONSOL) {
    check = !!(
      addressMapRes.cc_con_zip === '1' &&
      (addressMapRes.cc_con_address === '1' || addressMapRes.cc_con_google_match === '1')
    );
  } else if (type === types.MULTISTOP) {
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
  if (orderStatusRes) {
    return orderStatusRes;
  }
  const consolStatusRes = await getConsolStatus(shipmentId);
  if (consolStatusRes) {
    return { ...consolStatusRes, Type: types.MULTISTOP };
  }
  return {};
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
      ProjectionExpression: 'FK_OrderNo',
    };
    const result = await query(shipmentAparParams);
    console.info('🙂 -> file: dynamo.js:313 -> getAparDataByConsole -> result:', result);
    return result;
  } catch (err) {
    console.error('🙂 -> file: helper.js:546 -> err:', err);
    throw err;
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
    console.info('🙂 -> file: dynamo.js:338 -> getConsolStopHeader -> error:', error);
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
    console.info('🙂 -> file: dynamo.js:358 -> getShipmentForSeq -> error:', error);
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
    console.info('🙂 -> file: dynamo.js:358 -> getShipmentForSeq -> error:', error);
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
  getAparDataByConsole,
  getShipmentHeaderData,
  getConsolStopHeader,
  getShipmentForSeq,
  getShipmentForStop,
};
