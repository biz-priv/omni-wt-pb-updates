/*
 * File: src/movement/index.js
 * Project: PB-WT 214
 * Author: Bizcloud Experts
 * Date: 2024-08-14
 * Confidential and Proprietary
 */

'use strict';

const AWS = require('aws-sdk');
const _ = require('lodash');
const {
  getMovementOrder,
  getOrder,
  updateMilestone,
  getShipmentDetails,
} = require('../shared/dynamo');
const moment = require('moment-timezone');
const { publishSNSTopic } = require('../shared/apis');
const { milestones, deleteMassageFromQueue, status } = require('../shared/helper');

const { ADD_MILESTONE_TABLE_NAME, MOVEMENT_STREAM_QUEUE_URL } = process.env;

let functionName;
module.exports.handler = async (event, context) => {
  console.info('Event coming in ', event);
  functionName = _.get(context, functionName);

  try {
    const records = _.get(event, 'Records', []);
    const promises = records.map(async (oneRecord) => {
      let Id;
      let StatusCode;
      let Housebill = '';
      const receiptHandle = _.get(oneRecord, 'receiptHandle');
      try {
        const body = _.get(oneRecord, 'body', '');
        let { Message: record } = JSON.parse(body);
        record = JSON.parse(record);
        const newUnMarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
        const oldUnMarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);

        Id = _.get(newUnMarshalledRecord, 'id');
        console.info('id coming from movement table:', Id);

        const oldBrokerageStatus = _.get(oldUnMarshalledRecord, 'brokerage_status');
        const newBrokerageStatus = _.get(newUnMarshalledRecord, 'brokerage_status');
        console.info('oldBrokerageStatus:', oldBrokerageStatus);
        console.info('newBrokerageStatus:', newBrokerageStatus);

        const oldStatus = _.get(oldUnMarshalledRecord, 'status');
        const newStatus = _.get(newUnMarshalledRecord, 'status');
        console.info('oldStatus:', oldStatus);
        console.info('newStatus:', newStatus);

        console.info('brokerage_status coming from movement table is:', newBrokerageStatus);

        const orderId = await getMovementOrder(Id);
        console.info('🙂 -> file: index.js:50 -> promises -> orderId:', orderId);

        if (!orderId) return 'Movement Order table is not populated. Can not fetch order id.';

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

        if (oldBrokerageStatus !== 'DISPATCH' && newBrokerageStatus === 'DISPATCH') {
          StatusCode = milestones.TLD;

          console.info('Value of Id', Id);

          const finalPayload = {
            OrderId: orderId,
            StatusCode,
            Housebill: Housebill.toString(),
            EventDateTime: moment.tz('America/Chicago').format(),
            Payload: '',
            Response: '',
            ErrorMessage: '',
            Status: 'READY',
            Type: type,
          };

          console.info(finalPayload);
          await updateMilestone(finalPayload);
        }

        if (oldStatus === 'A' && newStatus === 'C') {
          StatusCode = milestones.BOO;

          console.info('Value of Id', Id);

          const finalPayload = {
            OrderId: orderId,
            StatusCode,
            Housebill: Housebill.toString(),
            EventDateTime: moment.tz('America/Chicago').format(),
            Payload: '',
            Response: '',
            ErrorMessage: '',
            Status: 'READY',
            Type: type,
          };

          console.info(finalPayload);
          await updateMilestone(finalPayload);
        }

        // if (oldStatus !== 'D' && newStatus === 'D' && ![types.MULTISTOP].includes(type)) {
        //   const movementId = Id;

        //   if (!movementId) {
        //     return {
        //       statusCode: 400,
        //       body: JSON.stringify('Movement ID is required'),
        //     };
        //   }

        //   const podStatus = await checkForPod(orderId);

        //   let resultMessage;

        //   if (podStatus === 'Y') {
        //     resultMessage = 'POD is Available';
        //     StatusCode = milestones.DEL;
        //   } else {
        //     resultMessage = 'POD is Unavailable';
        //     StatusCode = milestones.DWP;
        //   }

        //   console.info('WT status code :', StatusCode);
        //   console.info('resultMessage :', resultMessage);

        //   const finalPayload = {
        //     OrderId: orderId,
        //     StatusCode,
        //     Housebill: Housebill.toString(),
        //     EventDateTime: moment.tz('America/Chicago').format(),
        //     Payload: '',
        //     Response: '',
        //     ErrorMessage: '',
        //     Status: 'READY',
        //     Type: type,
        //   };

        //   console.info(finalPayload);
        //   await updateMilestone(finalPayload);
        // }
        return true;
      } catch (error) {
        await updateMilestone({
          EventDateTime: moment.tz('America/Chicago').format(),
          Housebill: Housebill.toString(),
          ErrorMessage: error.message,
          StatusCode,
          Status: status.FAILED,
        });

        await publishSNSTopic({
          id: Housebill,
          status: StatusCode,
          functionName,
          message: `Error processing Housebill: ${Housebill}.
          \n${error.message}.\n
          Please check the error message in DynamoDb Table ${ADD_MILESTONE_TABLE_NAME} for complete error`,
        });
        return await deleteMassageFromQueue({ queueUrl: MOVEMENT_STREAM_QUEUE_URL, receiptHandle });
      }
    });

    await Promise.all(promises);
  } catch (error) {
    console.error('Error in handler:', error);
    return false;
  }
  return true;
};
