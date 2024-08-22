'use strict';
const _ = require('lodash');
const AWS = require('aws-sdk');
const { deleteMassageFromQueue, types, status } = require('../shared/helper');
const { publishSNSTopicForLocationUpdate, addTrackingNote } = require('../shared/apis');
const { getShipmentDetails, insertOrUpdateLocationUpdateTable, getAparDataByConsole, getCallinDetails, getShipmentHeaderData, getLocationUpdateDetails, deleteDynamoRecord } = require('../shared/dynamo');
const moment = require('moment-timezone');

const { CALLIN_STABLE_QUEUE_URL, LOCATION_UPDATE_TABLE } = process.env;

let functionName;
module.exports.handler = async (event, context) => {
  functionName = context.functionName;
  console.info('🙂 -> file: index.js:5 -> module.exports.handler= -> functionName:', functionName);
  console.info('🙂 -> file: index.js:4 -> module.exports.handler= -> event:', event);

  const records = _.get(event, 'Records', []);

  if (_.get(records, 'eventName') === 'REMOVE') {
    console.info('SKipping Remove Event');
    return 'SKipping Remove Event';
  }

  const promises = records.map(async (oneRecord) => {
    const eventSource = _.get(oneRecord, 'eventSource');
    let body;
    let record;
    let receiptHandle;
    let callinId;
    let housebill;
    let orderId;
    let type;
    let newUnMarshalledRecord;
    let consoleNo = '0'
    try {
      if (eventSource === 'aws:sqs') {
        body = _.get(oneRecord, 'body', '');
        record = JSON.parse(_.get(JSON.parse(body), 'Message', ''));
        receiptHandle = _.get(oneRecord, 'receiptHandle');
        newUnMarshalledRecord = AWS.DynamoDB.Converter.unmarshall(
          _.get(record, 'dynamodb.NewImage')
        );
        console.info(
          '🙂 -> file: index.js:30 -> promises -> newUnMarshalledRecord:',
          newUnMarshalledRecord
        );
      }
      if (eventSource === 'aws:dynamodb') {
        record = oneRecord;
        const dynamoImage = AWS.DynamoDB.Converter.unmarshall(
          _.get(record, 'dynamodb.NewImage')
        );
        console.info('🙂 -> file: index.js:52 -> promises -> dynamoImage:', dynamoImage);
        const dynamoStreamHousebill = _.get(dynamoImage, 'Housebill')
        console.info('🙂 -> file: index.js:55 -> promises -> dynamoStreamHousebill:', dynamoStreamHousebill);
        const dynamoStreamShipmentType = _.get(dynamoImage, 'Type')
        console.info('🙂 -> file: index.js:55 -> promises -> dynamoStreamShipmentType:', dynamoStreamShipmentType);
        const callinIdForRetrigger = _.get(dynamoImage, 'CallinId');
        console.info('🙂 -> file: index.js:54 -> promises -> callinIdForRetrigger:', callinIdForRetrigger);
        if ([types.CONSOL, types.MULTISTOP].includes(dynamoStreamShipmentType)) {
          await deleteDynamoRecord({ pKeyName: 'Housebill', pKey: dynamoStreamHousebill, sKeyName: 'CallinId', sKey: callinIdForRetrigger, tableName: LOCATION_UPDATE_TABLE });
          console.info('Record deleted.');
        }
        if (!callinIdForRetrigger) return 'Callin Id not found.'
        const callinDetails = await getCallinDetails({ callinId: callinIdForRetrigger })
        console.info('🙂 -> file: index.js:56 -> promises -> callinDetails:', callinDetails);
        newUnMarshalledRecord = callinDetails
      }


      callinId = _.get(newUnMarshalledRecord, 'id', '');
      console.info('🙂 -> file: index.js:36 -> promises -> callinId:', callinId);
      orderId = _.get(newUnMarshalledRecord, 'order_id', '');
      console.info('🙂 -> file: index.js:38 -> promises -> orderId:', orderId);

      // const movementStatus = _.get(newUnMarshalledRecord, 'movement_status');
      // console.info('🙂 -> file: index.js:41 -> promises -> movementStatus:', movementStatus);

      // if (['C', 'P'].includes(movementStatus)) {
      //   console.info('The shipment status in not C or P. SKIPPING.');
      //   return 'The shipment status in not C or P. SKIPPING.';
      // }

      const city = _.get(newUnMarshalledRecord, 'city_name');
      console.info('🙂 -> file: index.js:53 -> promises -> city:', city);
      const state = _.get(newUnMarshalledRecord, 'state');
      console.info('🙂 -> file: index.js:55 -> promises -> state:', state);

      if (!city || !state) {
        console.info('Could not fetch city or state');
        return 'Could not fetch city or state';
      }

      const shipmentDetails = await getShipmentDetails({ shipmentId: orderId });
      housebill = _.get(shipmentDetails, 'housebill', '');
      console.info('🙂 -> file: index.js:43 -> promises -> housebill:', housebill);

      type = _.get(shipmentDetails, 'Type');
      console.info('🙂 -> file: index.js:45 -> promises -> type:', type);
      // throw new Error('Custom Error.')
      if (type === types.NON_CONSOL) {
        return await updateLocation({ housebill, city, state, callinId, orderId, receiptHandle, type });
      }

      if (type === types.CONSOL) {
        const consolidatedShipments = await getAparDataByConsole({ orderNo: housebill });
        console.info('🙂 -> file: index.js:79 -> promises -> consolidatedShipments:', consolidatedShipments);

        consoleNo = housebill;

        if (_.isEmpty(consolidatedShipments)) {
          console.info('No consolidated shipments found.');
          return 'No consolidated shipments found.';
        }

        for (const shipment of consolidatedShipments) {
          const shipmentHeaderData = await getShipmentHeaderData({
            orderNo: _.get(shipment, 'FK_OrderNo'),
          });
          console.info(
            '🙂 -> file: index.js:362 -> processForConsol -> shipmentHeaderData:',
            shipmentHeaderData
          );

          const consolidatedHousebill = _.get(shipmentHeaderData, '[0].Housebill');
          console.info('🙂 -> file: index.js:115 -> housebill:', housebill);

          await updateLocation({ housebill: consolidatedHousebill, city, state, callinId, orderId, receiptHandle, type, consoleNo });
        }
      }

      return true;
    } catch (error) {
      console.error('🙂 -> file: index.js:29 -> promises -> error:', error);
      console.error('Error in handler:', error);
      let message = _.get(error, 'message', '');
      let payload;
      let response;
      if (error.name === 'AxiosError') {
        payload = _.get(error, 'response', '');
        response = _.get(error, 'data', '');
        message = `Message: ${message}\nPayload: ${JSON.stringify(payload)}\nResponse: ${JSON.stringify(response)}`;
      }

      await publishSNSTopicForLocationUpdate({
        callinId,
        functionName,
        housebill,
        message,
        orderId,
      });

      await deleteMassageFromQueue({ queueUrl: CALLIN_STABLE_QUEUE_URL, receiptHandle });

      if (!housebill) return 'Could not fetch housebill number.';
      const data = {
        UpdatedAt: moment.tz('America/Chicago').format(),
        UpdatedBy: functionName,
        ErrorMessage: _.get(error, 'message'),
        Status: status.FAILED,
        Payload: payload,
        Response: response,
        Type: type,
        ConsolNo: consoleNo
      };
      return await insertOrUpdateLocationUpdateTable({
        housebill: housebill ?? callinId,
        callinId,
        data,
      });
    }
  });

  return await Promise.all(promises);
};

async function updateLocation({ city, state, housebill, callinId, orderId, receiptHandle, type, consoleNo = '0' }) {
  try {
    const existingRecord = await getLocationUpdateDetails({ housebill, callinId });
    if (_.get(existingRecord, 'Status') === status.SENT) return 'Location already sent. SKIPPING.';
    const { payload, response } = await addTrackingNote({ city, state, housebill });
    const data = {
      UpdatedAt: moment.tz('America/Chicago').format(),
      UpdatedBy: functionName,
      Status: status.SENT,
      Payload: payload,
      Response: response,
      Message: 'Location updated successfully',
      ShipmentId: orderId,
      Type: type,
      ConsolNo: consoleNo
    };
    return await insertOrUpdateLocationUpdateTable({
      housebill,
      callinId,
      data,
    });
  } catch (error) {
    console.info('🙂 -> file: index.js:112 -> updateLocation -> error:', error);
    let message = _.get(error, 'message', 'Location Update failed.');
    let payload;
    let response;
    if (error.name === 'AxiosError') {
      payload = _.get(error, 'data', '');
      response = _.get(error, 'response', '');
      message = `Message: ${message}\nPayload: ${JSON.stringify(payload)}\nResponse: ${JSON.stringify(response)}`;
    }
    await publishSNSTopicForLocationUpdate({ callinId, functionName, housebill, message, orderId });
    await deleteMassageFromQueue({ queueUrl: CALLIN_STABLE_QUEUE_URL, receiptHandle });
    const data = {
      UpdatedAt: moment.tz('America/Chicago').format(),
      UpdatedBy: functionName,
      Status: status.FAILED,
      Payload: payload,
      Response: response,
      Message: message,
      ShipmentId: orderId,
      ConsolNo: consoleNo
    };
    return await insertOrUpdateLocationUpdateTable({
      housebill,
      callinId,
      data,
    });
  }
}
