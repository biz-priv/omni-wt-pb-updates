/* eslint-disable consistent-return */
'use strict';
const AWS = require('aws-sdk');
const _ = require('lodash');
const {
  getOrder,
  updateMilestone,
  getMovement,
  getStop,
  getShipmentDetails,
} = require('../shared/dynamo');
const moment = require('moment-timezone');
const { getOrders, checkForPod, publishSNSTopic } = require('../shared/apis');
const { types, milestones } = require('../shared/helper');

const { ADD_MILESTONE_TABLE_NAME } = process.env;

exports.handler = async (event) => {
  console.info('Event: ', JSON.stringify(event));

  const records = _.get(event, 'Records', []);
  if (_.get(records, 'eventName') === 'INSERT' || _.get(records, 'eventName') === 'REMOVE') {
    console.info('SKipping Insert/Remove Event');
    return;
  }
  const promises = records.map(async (record) => {
    let stopId;
    let StatusCode;
    let Housebill;
    let orderId;
    try {
      const newUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
      const oldUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);

      stopId = _.get(newUnmarshalledRecord, 'id', '');
      console.info('id coming from stop table:', stopId);

      const oldActualArrival = _.get(oldUnmarshalledRecord, 'actual_arrival', '');
      const newActualArrival = _.get(newUnmarshalledRecord, 'actual_arrival', '');
      const newStopType = _.get(newUnmarshalledRecord, 'stop_type', '');

      const oldActualDeparture = _.get(oldUnmarshalledRecord, 'actual_departure', '');
      const newActualDeparture = _.get(newUnmarshalledRecord, 'actual_departure', '');

      const oldConfirmed = _.get(oldUnmarshalledRecord, 'confirmed', '');
      const newConfirmed = _.get(newUnmarshalledRecord, 'confirmed', '');

      console.info('Old Actual Arrival: ', oldActualArrival);
      console.info('New Actual Arrival: ', newActualArrival);

      console.info('Old Actual Departure: ', oldActualDeparture);
      console.info('New Actual Departure: ', newActualDeparture);

      console.info('Old Confirmed: ', oldConfirmed);
      console.info('New Confirmed: ', newConfirmed);

      console.info('New Stop Type: ', newStopType);

      orderId = _.get(newUnmarshalledRecord, 'order_id', '');
      console.info('Order Id coming from the Event:', orderId);

      Housebill = await getOrder(orderId);

      const shipmentDetails = await getShipmentDetails({ shipmentId: orderId });
      console.info(
        '🙂 -> file: index.js:45 -> promises -> shipmentDetails:',
        JSON.stringify(shipmentDetails)
      );

      const type = _.get(shipmentDetails, 'Type');
      console.info('🙂 -> file: index.js:48 -> promises -> type:', type);
      if (!type || !shipmentDetails) {
        console.info('Shipment is not created through our system. SKIPPING.');
        return 'Shipment is not created through our system. SKIPPING.';
      }

      const totalSequenceSteps = await getStop(orderId);
      const maxSequenceId = _.size(totalSequenceSteps);
      console.info('Number of Records in Stop table for this record:', maxSequenceId); // max value

      const seqId = await _.get(newUnmarshalledRecord, 'movement_sequence', '');
      console.info('Sequence Id for the Record from Stop table for this record:', seqId); // seqId

      // Status Code = APL
      if (
        (oldActualArrival === '' || oldActualArrival === null) &&
        newActualArrival !== null &&
        newActualArrival !== '' &&
        newStopType === 'PU'
      ) {
        console.info('The First Pickup of the Consolidation');
        StatusCode = milestones.APL;
        if ([types.MULTISTOP].includes(type)) StatusCode = `${milestones.APL}#${seqId}`;
        console.info('Sending Status Code: ', StatusCode);
        const finalPayload = await getPayloadForStopDb(
          StatusCode,
          stopId,
          newStopType,
          type,
          orderId,
          Housebill
        );
        await updateMilestone(finalPayload);
      }

      // status Code = TTC
      if (
        (oldActualDeparture === null || oldActualDeparture === '') &&
        newActualDeparture !== null &&
        newActualDeparture !== '' &&
        newStopType === 'PU'
      ) {
        StatusCode = milestones.TTC;
        if ([types.MULTISTOP].includes(type)) StatusCode = `${milestones.TTC}#${seqId}`;
        console.info('Sending Status Code: ', StatusCode);
        let finalPayload = await getPayloadForStopDb(
          StatusCode,
          stopId,
          newStopType,
          type,
          orderId,
          Housebill
        );
        await updateMilestone(finalPayload);
        // adding COB/IN Transit
        StatusCode = milestones.COB;
        if ([types.MULTISTOP].includes(type)) StatusCode = `${milestones.COB}#${seqId}`;
        console.info('Sending Status Code: ', StatusCode);
        finalPayload = await getPayloadForStopDb(
          StatusCode,
          stopId,
          newStopType,
          type,
          orderId,
          Housebill
        );
        await updateMilestone(finalPayload);
      }

      // status Code = AAD
      if (
        (oldActualArrival === '' || oldActualArrival === null) &&
        newActualArrival !== null &&
        newActualArrival !== '' &&
        newStopType === 'SO'
      ) {
        console.info('The Last Delivery of the Consolidation');
        StatusCode = milestones.AAD;
        if ([types.MULTISTOP].includes(type)) StatusCode = `${milestones.AAD}#${seqId}`;
        console.info('Sending Status Code: ', StatusCode);
        const finalPayload = await getPayloadForStopDb(
          StatusCode,
          stopId,
          newStopType,
          type,
          orderId,
          Housebill
        );
        await updateMilestone(finalPayload);
      }

      // status Code = DWP or DEL
      if (
        (oldActualDeparture === null || oldActualDeparture === '') &&
        newActualDeparture !== null &&
        newActualDeparture !== '' &&
        newStopType === 'SO' &&
        [types.MULTISTOP].includes(type)
      ) {
        const podStatus = await checkForPod(orderId);

        let resultMessage;

        if (podStatus === 'Y') {
          resultMessage = 'POD is Available';
          StatusCode = milestones.DEL;
        } else {
          resultMessage = 'POD is Unavailable';
          StatusCode = milestones.DWP;
        }

        console.info('WT status code :', StatusCode);
        console.info('resultMessage :', resultMessage);
        StatusCode = `${StatusCode}#${seqId}`;
        console.info('Sending Status Code: ', StatusCode);
        const finalPayload = await getPayloadForStopDb(
          StatusCode,
          stopId,
          newStopType,
          type,
          orderId,
          Housebill
        );
        await updateMilestone(finalPayload);
      }

      // status Code = APP
      if (oldConfirmed !== 'Y' && newConfirmed === 'Y' && newStopType === 'PU') {
        StatusCode = milestones.APP;
        if ([types.MULTISTOP].includes(type)) StatusCode = `${milestones.APP}#${seqId}`;
        console.info('Sending Status Code: ', StatusCode);
        console.info(
          '🙂 -> file: index.js:140 -> promises -> StatusCode, stopId, newStopType:',
          StatusCode,
          stopId,
          newStopType
        );
        const finalPayload = await getPayloadForStopDb(
          StatusCode,
          stopId,
          newStopType,
          type,
          orderId,
          Housebill
        );
        await updateMilestone(finalPayload);
      }

      // status Code = APD
      if (oldConfirmed !== 'Y' && newConfirmed === 'Y' && newStopType === 'SO') {
        StatusCode = milestones.APD;
        if ([types.MULTISTOP].includes(type)) StatusCode = `${milestones.APD}#${seqId}`;
        console.info('Sending Status Code: ', StatusCode);
        const finalPayload = await getPayloadForStopDb(
          StatusCode,
          stopId,
          newStopType,
          type,
          orderId,
          Housebill
        );
        await updateMilestone(finalPayload);
      }
    } catch (error) {
      console.error('Error in handler:', error);
      // id, statuscode, housebill, eventdatetime, sattus,
      await updateMilestone({
        OrderId: orderId,
        EventDateTime: moment.tz('America/Chicago').format(),
        Housebill: Housebill?.toString(),
        ErrorMessage: error.message,
        StatusCode: StatusCode ?? 'FAILED',
        Status: 'FAILED',
      });
      // add sns here
      await publishSNSTopic({
        id: orderId,
        status: StatusCode,
        message: `Error processing StopId: ${stopId}, ${error.message}. \n Please check the error meesage in DynamoDb Table ${ADD_MILESTONE_TABLE_NAME} for complete error`,
      });
      throw error;
    }
  });

  await Promise.all(promises);
};

async function getPayloadForStopDb(StatusCode, stopId, stopType, type, orderId, housebill) {
  try {
    let modifiedStopId = stopId;
    let lastStop;
    if ([types.MULTISTOP].includes(type)) {
      const orderDetails = await getOrders({ id: orderId });
      console.info('🙂 -> file: index.js:253 -> getPayloadForStopDb -> stops:', orderDetails);
      const stops = _.get(orderDetails, 'stops', []);
      console.info('🙂 -> file: index.js:256 -> getPayloadForStopDb -> stops:', stops);
      const firstStop = _.get(stops, '[0].id');
      console.info('🙂 -> file: index.js:204 -> getPayloadForStopDb -> firstStop:', firstStop);
      lastStop = _.get(stops, `[${_.size(stops) - 1}].id`);
      console.info('🙂 -> file: index.js:261 -> getPayloadForStopDb -> lastStop:', lastStop);
      modifiedStopId = stopType === 'PU' ? firstStop : lastStop;
    }
    if (modifiedStopId !== lastStop) await getMovement(modifiedStopId, stopType);

    const finalPayload = {
      OrderId: orderId,
      StatusCode,
      Housebill: housebill.toString(),
      EventDateTime: moment.tz('America/Chicago').format(),
      Payload: '',
      Response: '',
      ErrorMessage: '',
      Status: 'READY',
      Type: type,
    };

    console.info('Payload for add milestone:', finalPayload);
    return finalPayload;
  } catch (error) {
    console.error('Error in getPayloadForStopDb function: ', error);
    throw error;
  }
}
