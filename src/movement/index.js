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

const sns = new AWS.SNS();
const axios = require('axios');

const { ERROR_SNS_TOPIC_ARN, ADD_MILESTONE_TABLE_NAME, ENVIRONMENT } = process.env;

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
        StatusCode = 'TLD';

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
        StatusCode = 'BOO';

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

      if (oldStatus !== 'D' && newStatus === 'D' && !['MULTI-STOP', 'CONSOL'].includes(type)) {
        const movementId = Id;

        if (!movementId) {
          return {
            statusCode: 400,
            body: JSON.stringify('Movement ID is required'),
          };
        }

        const podStatus = await checkForPod(movementId, orderId);

        let resultMessage;

        if (podStatus === 'Y') {
          resultMessage = 'POD is Available';
          StatusCode = 'DEL';
        } else {
          resultMessage = 'POD is Unavailable';
          StatusCode = 'DWP';
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

async function publishSNSTopic({ subject, message }) {
  try {
    const params = {
      TopicArn: ERROR_SNS_TOPIC_ARN,
      Subject: subject,
      Message: message,
    };

    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}

async function checkForPod(movementId, orderId) {
  try {
    const username = 'apiuser';
    const password = 'lvlpapiuser';
    const mcleodHeaders = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    };
    const rowType = 'O';

    console.info('movementID is ', movementId);
    console.info('orderId is ', orderId);
    console.info('Entered POD STATUS CHECK function');

    // Basic Auth header
    const authHeader = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;

    // Get POD
    const url = `https://tms-lvlp.loadtracking.com:6790/ws/api/images/${rowType}/${orderId}`;

    console.info('URL trying to fetch the details:', url);

    const response = await axios.get(url, {
      headers: { ...mcleodHeaders, Authorization: authHeader },
    });

    console.info('RESPONSE: ', response);
    console.info('RESPONSE Status: ', response.status);

    if (response.status !== 200) {
      const errorMessage = `Failed to get POD for order ${orderId}. Status code: ${response.status}`;
      await publishSNSTopic({
        subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ id: ${movementId}`,
        message: errorMessage,
      });
      return 'N';
    }

    console.info('1st If Condition passed');

    const output = response.data;

    console.info('Output tagged:', output);

    let exists;

    if (!output) throw new Error('Empty response');

    const photoType = output[0].descr.toUpperCase();

    // Check to see if there is a POD
    if (photoType === '01-BILL OF LADING') {
      exists = 'Y';
    } else {
      exists = 'N';
    }
    console.info('value in exists :', exists);
    console.info(`Does a POD for movement ${movementId} exist? ${exists}`);
    return exists;
  } catch (error) {
    const errorMessage = `Error processing POD data: ${error.message}`;
    await publishSNSTopic({
      subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ id: ${movementId}`,
      message: errorMessage,
    });
  }
  return true;
}
