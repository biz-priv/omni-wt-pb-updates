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
const { checkForPod, publishSNSTopic } = require('../shared/apis');
const { types, milestones } = require('../shared/helper');

const { ADD_MILESTONE_TABLE_NAME, ENVIRONMENT } = process.env;

module.exports.handler = async (event) => {
  let Id;
  let StatusCode;
  let Housebill;

  console.info('Event coming in ', event);

  try {
    const records = _.get(event, 'Records', []);
    const promises = records.map(async (record) => {
      const newUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
      const oldUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);

      Id = _.get(newUnmarshalledRecord, 'id');
      console.info('id coming from movement table:', Id);

      const oldBrokerageStatus = _.get(oldUnmarshalledRecord, 'brokerage_status');
      const newBrokerageStatus = _.get(newUnmarshalledRecord, 'brokerage_status');
      console.info('oldBrokerageStatus:', oldBrokerageStatus);
      console.info('newBrokerageStatus:', newBrokerageStatus);

      const oldStatus = _.get(oldUnmarshalledRecord, 'status');
      const newStatus = _.get(newUnmarshalledRecord, 'status');
      console.info('oldStatus:', oldStatus);
      console.info('newStatus:', newStatus);

      console.info('brokerage_status coming from movement table is:', newBrokerageStatus);

      const orderId = await getMovementOrder(Id);
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

      if (
        oldStatus !== 'D' &&
        newStatus === 'D' &&
        ![types.MULTISTOP, types.CONSOL].includes(type)
      ) {
        const movementId = Id;

        if (!movementId) {
          return {
            statusCode: 400,
            body: JSON.stringify('Movement ID is required'),
          };
        }

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
      return true;
    });

    await Promise.all(promises);
  } catch (error) {
    console.error('Error in handler:', error);

    await updateMilestone({
      EventDateTime: moment.tz('America/Chicago').format(),
      Housebill: Housebill.toString(),
      ErrorMessage: error.message,
      StatusCode: 'FAILED',
    });

    await publishSNSTopic({
      subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ id: ${Id}`,
      message: `Error processing Housebill: ${Housebill}, ${error.message}. \n Please check the error meesage in DynamoDb Table ${ADD_MILESTONE_TABLE_NAME} for complete error`,
    });
    throw error;
  }
};
