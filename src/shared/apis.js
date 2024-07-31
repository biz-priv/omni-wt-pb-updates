'use strict';

const { default: axios } = require('axios');
const _ = require('lodash');
const AWS = require('aws-sdk');

const {
  GET_ORDERS_API_ENDPOINT,
  AUTH,
  ENVIRONMENT,
  ERROR_SNS_TOPIC_ARN,
  CHECK_POD_API_ENDPOINT,
  PB_USERNAME,
  PB_PASSWORD,
} = process.env;

const sns = new AWS.SNS();

async function getOrders({ id }) {
  const apiUrl = `${GET_ORDERS_API_ENDPOINT}/${id}`;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.get(apiUrl, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:30 -> getOrders -> responseData:', responseData);
    // Return the location ID or perform additional processing as needed
    return responseData;
  } catch (error) {
    console.error('🙂 -> file: apis.js:34 -> getOrders -> error:', error);
    return false;
  }
}

async function checkForPod(orderId) {
  try {
    const username = PB_USERNAME;
    const password = PB_PASSWORD;
    const mcleodHeaders = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    };
    const rowType = 'O';

    console.info('orderId is ', orderId);
    console.info('Entered POD STATUS CHECK function');

    // Basic Auth header
    const authHeader = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;

    // Get POD
    const url = `${CHECK_POD_API_ENDPOINT}/${rowType}/${orderId}`; // TODO: Move the url to ssm

    console.info('URL trying to fetch the details:', url);

    const response = await axios.get(url, {
      headers: { ...mcleodHeaders, Authorization: authHeader },
    });

    console.info('RESPONSE: ', response);
    console.info('RESPONSE Status: ', response.status);

    if (response.status !== 200) {
      const errorMessage = `Failed to get POD for order ${orderId}. Status code: ${response.status}`;
      await publishSNSTopic({
        id: orderId,
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
    console.info(`Does a POD for order ${orderId} exist? ${exists}`);
    return exists;
  } catch (error) {
    const errorMessage = `Error processing POD data: ${error.message}`;
    await publishSNSTopic({
      id: orderId,
      message: errorMessage,
    });
  }
  return true;
}

async function publishSNSTopic({ id, status, message }) {
  const subject = `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ id: ${id}, status: ${status}`;
  try {
    const params = {
      TopicArn: ERROR_SNS_TOPIC_ARN,
      Subject: subject,
      Message: message,
    };
    console.info('🙂 -> file: apis.js:118 -> publishSNSTopic -> params:', params);
    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}

module.exports = {
  getOrders,
  checkForPod,
  publishSNSTopic,
};
