'use strict';
const _ = require('lodash');
const AWS = require('aws-sdk');
const { deleteMassageFromQueue, types, status } = require('../shared/helper');
const { publishSNSTopicForLocationUpdate, addTrackingNote } = require('../shared/apis');
const {
  getShipmentDetails,
  insertOrUpdateLocationUpdateTable,
  getAparDataByConsole,
  getCallinDetails,
  getShipmentHeaderData,
  getLocationUpdateDetails,
  deleteDynamoRecord,
  getShipmentForStop,
  getStopDetails,
  getTotalStop,
} = require('../shared/dynamo');
const moment = require('moment-timezone');

const { CALLIN_STABLE_QUEUE_URL, LOCATION_UPDATE_TABLE } = process.env;

let functionName;
module.exports.handler = async (event, context) => {
  functionName = context.functionName;
  console.info('🙂 -> file: index.js:5 -> module.exports.handler= -> functionName:', functionName);
  console.info('🙂 -> file: index.js:4 -> module.exports.handler= -> event:', event);

  const records = _.get(event, 'Records', []);

  //* Do not do anything for delete event
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
    let consolNo = '0';
    try {
      //* The main process comes from aws sqs
      if (eventSource === 'aws:sqs') {
        body = _.get(oneRecord, 'body', '');
        record = JSON.parse(_.get(JSON.parse(body), 'Message', ''));
        receiptHandle = _.get(oneRecord, 'receiptHandle');
        newUnMarshalledRecord = AWS.DynamoDB.Converter.unmarshall(
          _.get(record, 'dynamodb.NewImage')
        );
        console.info(
          '🙂 -> file: index.js:46 -> promises -> newUnMarshalledRecord:',
          newUnMarshalledRecord
        );
      }

      //* Process retrigger from dynamodb stream
      if (eventSource === 'aws:dynamodb') {
        record = oneRecord;
        const dynamoImage = AWS.DynamoDB.Converter.unmarshall(_.get(record, 'dynamodb.NewImage'));
        console.info('🙂 -> file: index.js:57 -> promises -> dynamoImage:', dynamoImage);

        const dynamoStreamHousebill = _.get(dynamoImage, 'Housebill');
        console.info(
          '🙂 -> file: index.js:60 -> promises -> dynamoStreamHousebill:',
          dynamoStreamHousebill
        );

        const dynamoStreamShipmentType = _.get(dynamoImage, 'Type');
        console.info(
          '🙂 -> file: index.js:63 -> promises -> dynamoStreamShipmentType:',
          dynamoStreamShipmentType
        );

        const callinIdForRetrigger = _.get(dynamoImage, 'CallinId');
        console.info(
          '🙂 -> file: index.js:54 -> promises -> callinIdForRetrigger:',
          callinIdForRetrigger
        );

        //* Delete the failed record. The record will be added after it is processed.
        await deleteDynamoRecord({
          pKeyName: 'Housebill',
          pKey: dynamoStreamHousebill,
          sKeyName: 'CallinId',
          sKey: callinIdForRetrigger,
          tableName: LOCATION_UPDATE_TABLE,
        });
        console.info('Record deleted.');

        if (!callinIdForRetrigger) return 'Callin Id not found.';

        //* Get the record from callin database
        const callinDetails = await getCallinDetails({ callinId: callinIdForRetrigger });
        console.info('🙂 -> file: index.js:56 -> promises -> callinDetails:', callinDetails);

        newUnMarshalledRecord = callinDetails;
      }

      callinId = _.get(newUnMarshalledRecord, 'id', '');
      console.info('🙂 -> file: index.js:83 -> promises -> callinId:', callinId);

      orderId = _.get(newUnMarshalledRecord, 'order_id', '');
      console.info('🙂 -> file: index.js:86 -> promises -> orderId:', orderId);

      const city = _.get(newUnMarshalledRecord, 'city_name');
      console.info('🙂 -> file: index.js:89 -> promises -> city:', city);

      const state = _.get(newUnMarshalledRecord, 'state');
      console.info('🙂 -> file: index.js:92 -> promises -> state:', state);

      const stopId = _.get(newUnMarshalledRecord, 'current_stop_id');
      console.info('🙂 -> file: index.js:95 -> promises -> stopId:', stopId);

      if (!city || !state) {
        console.info('Could not fetch city or state');
        return 'Could not fetch city or state';
      }

      //* Check the shipment in order-status table and console-status table to get some required table.
      const shipmentDetails = await getShipmentDetails({ shipmentId: orderId });
      housebill = _.get(shipmentDetails, 'housebill', '');
      console.info('🙂 -> file: index.js:43 -> promises -> housebill:', housebill);

      type = _.get(shipmentDetails, 'Type');
      console.info('🙂 -> file: index.js:45 -> promises -> type:', type);

      if (type === types.NON_CONSOL) {
        return await updateLocation({
          housebill,
          city,
          state,
          callinId,
          orderId,
          receiptHandle,
          type,
        });
      }

      if (type === types.CONSOL) {
        //* Get the consolidated shipment using the consol number
        const consolidatedShipments = await getAparDataByConsole({ orderNo: housebill });
        console.info(
          '🙂 -> file: index.js:79 -> promises -> consolidatedShipments:',
          consolidatedShipments
        );

        consolNo = housebill;

        if (_.isEmpty(consolidatedShipments)) {
          console.info('No consolidated shipments found.');
          return 'No consolidated shipments found.';
        }

        //* Add tracking for individual shipment.
        for (const shipment of consolidatedShipments) {
          //* Get housebill from shipment-header table using order id.
          const shipmentHeaderData = await getShipmentHeaderData({
            orderNo: _.get(shipment, 'FK_OrderNo'),
          });
          console.info(
            '🙂 -> file: index.js:362 -> processForConsol -> shipmentHeaderData:',
            shipmentHeaderData
          );

          const consolidatedHousebill = _.get(shipmentHeaderData, '[0].Housebill');
          console.info('🙂 -> file: index.js:115 -> housebill:', housebill);

          await updateLocation({
            housebill: consolidatedHousebill,
            city,
            state,
            callinId,
            orderId,
            receiptHandle,
            type,
            consolNo,
          });
        }
      }

      if (type === types.MULTISTOP && stopId) {
        consolNo = housebill;

        //* Get total stop for the consolidation.
        const totalStopRes = await getTotalStop({ consolNo });
        const totalStop = totalStopRes?.length;
        console.info(
          '🙂 -> file: index.js:204 -> module.exports.handler= -> totalStop:',
          totalStop
        );

        //* Get the current stop seq for this callin
        const stopDetails = await getStopDetails({ id: stopId });
        console.info('🙂 -> file: index.js:139 -> promises -> stopDetails:', stopDetails);
        const stopSeq = parseInt(_.get(stopDetails, 'movement_sequence'), 10);
        console.info('🙂 -> file: index.js:155 -> promises -> stopSeq:', stopSeq);

        let consolidatedShipments = [];

        //* Send the location updates to the remaining stops including current stop
        for (let index = stopSeq; index <= totalStop; index++) {
          let consolidatedShipmentsRes = await getShipmentForStop({ consolNo, stopSeq: index });
          console.info(
            `🙂 -> file: index.js:162 -> promises -> consolidatedShipmentsRes -> StopSeq -> ${index}:`,
            consolidatedShipmentsRes
          );
          consolidatedShipmentsRes = consolidatedShipmentsRes.map((shipment) =>
            _.get(shipment, 'FK_OrderNo')
          );
          consolidatedShipments = [...consolidatedShipments, ...consolidatedShipmentsRes];
        }

        consolidatedShipments = Array.from(new Set(consolidatedShipments));

        console.info(
          '🙂 -> file: index.js:159 -> promises -> consolidatedShipments:',
          consolidatedShipments
        );

        for (const shipment of consolidatedShipments) {
          const shipmentHeaderData = await getShipmentHeaderData({
            orderNo: shipment,
          });
          console.info(
            '🙂 -> file: index.js:362 -> processForConsol -> shipmentHeaderData:',
            shipmentHeaderData
          );

          const consolidatedHousebill = _.get(shipmentHeaderData, '[0].Housebill');
          console.info('🙂 -> file: index.js:115 -> housebill:', housebill);

          await updateLocation({
            housebill: consolidatedHousebill,
            city,
            state,
            callinId,
            orderId,
            receiptHandle,
            type,
            consolNo,
          });
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
        housebill: housebill ?? callinId,
        message,
        orderId,
      });

      await deleteMassageFromQueue({ queueUrl: CALLIN_STABLE_QUEUE_URL, receiptHandle });

      const data = {
        UpdatedAt: moment.tz('America/Chicago').format(),
        UpdatedBy: functionName,
        ErrorMessage: _.get(error, 'message'),
        Status: status.FAILED,
        Payload: payload,
        Response: response,
        Type: type,
        ConsolNo: consolNo,
      };
      return await insertOrUpdateLocationUpdateTable({
        housebill: housebill ?? callinId, //* If housebill not available, set the callin id as housebill (hash key) else the update will fail.
        callinId,
        data,
      });
    }
  });

  return await Promise.all(promises);
};

async function updateLocation({
  city,
  state,
  housebill,
  callinId,
  orderId,
  receiptHandle,
  type,
  consolNo = '0',
}) {
  try {
    const existingRecord = await getLocationUpdateDetails({ housebill, callinId });
    //* Check if the location is already sent, If it is already sent; do not send it again.
    if (_.get(existingRecord, 'Status') === status.SENT) return 'Location already sent. SKIPPING.';

    const { payload, response } = await addTrackingNote({ city, state, housebill });
    console.info('🙂 -> file: index.js:240 -> updateLocation -> response:', response);
    console.info('🙂 -> file: index.js:240 -> updateLocation -> payload:', payload);

    const data = {
      UpdatedAt: moment.tz('America/Chicago').format(),
      UpdatedBy: functionName,
      Status: status.SENT,
      Payload: payload,
      Response: response,
      Message: 'Location updated successfully',
      ShipmentId: orderId,
      Type: type,
      ConsolNo: consolNo,
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
      ConsolNo: consolNo,
    };
    return await insertOrUpdateLocationUpdateTable({
      housebill,
      callinId,
      data,
    });
  }
}
