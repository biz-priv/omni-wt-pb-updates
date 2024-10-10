/*
 * File: src/shared/apis.js
 * Project: PB-WT 214
 * Author: Bizcloud Experts
 * Date: 2024-08-14
 * Confidential and Proprietary
 */

'use strict';

const { default: axios } = require('axios');
const _ = require('lodash');
const AWS = require('aws-sdk');
const { convert } = require('xmlbuilder2');
const { CustomAxiosError, executePreparedStatement } = require('./helper');

const {
  GET_ORDERS_API_ENDPOINT,
  AUTH,
  ENVIRONMENT,
  ERROR_SNS_TOPIC_ARN,
  CHECK_POD_API_ENDPOINT,
  ADD_MILESTONE_URL,
  ADD_DOCUMENT_URL,
  ADD_DOCUMENT_API_KEY,
  LOCATION_UPDATE_TABLE,
  WT_SOAP_USERNAME,
  WT_SOAP_PASSWORD,
  TRACKING_NOTES_API_URL,
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
    const responseData = _.get(response, 'data', {});
    return responseData;
  } catch (error) {
    console.error('🙂 -> file: apis.js:34 -> getOrders -> error:', error);
    return false;
  }
}

async function checkForPod(orderId) {
  try {
    const mcleodHeaders = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    };
    const rowType = 'O';

    console.info('orderId is ', orderId);
    console.info('Entered POD STATUS CHECK function');

    // Get POD
    const url = `${CHECK_POD_API_ENDPOINT}/${rowType}/${orderId}`;

    console.info('URL trying to fetch the details:', url);

    const response = await axios.get(url, {
      headers: { ...mcleodHeaders, Authorization: AUTH },
    });

    console.info('RESPONSE: ', response);
    console.info('RESPONSE Status: ', response.status);

    if (response.status !== 200) {
      const errorMessage = `Failed to get POD for order ${orderId}. Status code: ${response.status}`;
      console.info('🙂 -> file: apis.js:75 -> checkForPod -> errorMessage:', errorMessage);
      return 'N';
    }

    console.info('1st If Condition passed');

    const output = response.data;

    console.info('Output tagged:', output);

    let exists;

    if (!output) return 'N';

    const photoType = _.get(output, '[0].descr', '').toUpperCase();

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
    console.error('🙂 -> file: apis.js:103 -> checkForPod -> error:', error);
    return 'N';
  }
}

async function publishSNSTopic({ id, status, message, functionName }) {
  const subject = `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ id: ${id}, status: ${status}`;
  try {
    const logStreamName = await getCurrentLogGroupName({ functionName });
    const logUrl = getLogUrl({ functionName, logStreamName });
    const params = {
      TopicArn: ERROR_SNS_TOPIC_ARN,
      Subject: subject,
      Message: ` Function name: ${functionName}\n${message}\nLog URL: ${logUrl}\nCheck the log for more detailed error.`,
    };
    console.info('🙂 -> file: apis.js:118 -> publishSNSTopic -> params:', params);
    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}

async function getPODId(orderId) {
  try {
    const mcleodHeaders = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    };
    const rowType = 'O';

    console.info('orderId is ', orderId);
    console.info('Entered POD STATUS CHECK function');

    // Get POD
    // const url = 'https://tms-lvlp.loadtracking.com/ws/api/images/O/0221320';
    const url = `${CHECK_POD_API_ENDPOINT}/${rowType}/${orderId}`;

    console.info('URL trying to fetch the details:', url);

    const response = await axios.get(url, {
      headers: { ...mcleodHeaders, Authorization: AUTH },
    });

    console.info('RESPONSE: ', response);
    console.info('RESPONSE Status: ', response.status);

    if (response.status !== 200) {
      return false;
    }

    const output = response.data;

    console.info('Output tagged:', output);

    if (!output) return false;

    const filteredResponse = output.filter(
      (item) => _.get(item, 'descr', '').toUpperCase() === '01-BILL OF LADING'
    );
    console.info('🙂 -> file: apis.js:173 -> getPODId -> filteredResponse:', filteredResponse);

    if (!filteredResponse.length) return false;

    const photoType = filteredResponse[0]?.descr?.toUpperCase();
    console.info('🙂 -> file: apis.js:171 -> getPODId -> photoType:', photoType);

    return filteredResponse[0];
  } catch (error) {
    console.error('🙂 -> file: apis.js:186 -> getPODId -> error:', error);
    return false;
  }
}

async function getPOD(imageId) {
  try {
    const mcleodHeaders = {
      Accept: 'application/pdf',
    };

    console.info('imageId is: ', imageId);

    // Get POD
    // const url = `https://tms-lvlp.loadtracking.com/ws/api/images/${imageId}`;
    const url = `${CHECK_POD_API_ENDPOINT}/${imageId}`;

    console.info('URL trying to fetch the details:', url);

    const response = await axios.get(url, {
      headers: { ...mcleodHeaders, Authorization: AUTH },
      responseType: 'arraybuffer',
    });

    const output = response.data;
    console.info('🙂 -> file: apis.js:210 -> getPOD -> typeof output:', typeof output);

    return output;
  } catch (error) {
    console.error('🙂 -> file: apis.js:218 -> getPOD -> error:', error);
    throw error;
  }
}

async function uploadPODDoc({ housebill, base64 }) {
  try {
    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: ADD_DOCUMENT_URL,
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ADD_DOCUMENT_API_KEY,
      },
      data: {
        UploadPODDocument: {
          Housebill: housebill,
          b64str: base64,
        },
      },
    };
    console.info(`Uploading POD for housebill: ${housebill}`);
    const podUploadRes = await axios.request(config);
    console.info('🙂 -> file: apis.js:241 -> uploadPODDoc -> podUploadRes:', podUploadRes.data);
    return podUploadRes.data?.msg === 'Success';
  } catch (error) {
    console.error('🙂 -> file: apis.js:242 -> uploadPODDoc -> error:', error);
    throw error;
  }
}

async function markAsDelivered(housebill, EventDateTime) {
  try {
    const data = `<?xml version="1.0" encoding="utf-8"?>\n<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">\n    <soap12:Body>\n        <SubmitPOD xmlns="http://tempuri.org/">\n            <HAWB>${housebill}</HAWB>\n            <UserName>${WT_SOAP_USERNAME}</UserName>\n            <UserInitials>${WT_SOAP_USERNAME}</UserInitials>\n            <Signer>see_pod</Signer>\n            <PODDateTime>${EventDateTime}</PODDateTime>\n            <LatLon>0,0</LatLon>\n        </SubmitPOD>\n    </soap12:Body>\n</soap12:Envelope>`;

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: ADD_MILESTONE_URL,
      headers: {
        'Content-Type': 'text/xml',
      },
      data,
    };
    console.info('🙂 -> file: apis.js:253 -> markAsDelivered -> config:', config);
    const res = await axios.request(config);
    if (_.get(res, 'status', '') === 200) {
      return _.get(res, 'data', '');
    }

    throw new Error(`API Request Failed: ${JSON.stringify(res)}`);
  } catch (error) {
    console.error('��� -> file: apis.js:283 -> markAsDelivered -> error:', error);
    throw error;
  }
}

async function uploadPOD({ housebill, base64 }) {
  try {
    const data = makeJsonToXml({ base64, housebill });

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: ADD_MILESTONE_URL,
      headers: {
        'Content-Type': 'application/soap+xml; charset=utf-8',
      },
      data,
    };
    console.info('🙂 -> file: apis.js:253 -> markAsDelivered -> config:', config);
    const res = await axios.request(config);
    if (_.get(res, 'status', '') === 200) {
      return _.get(res, 'data', '');
    }

    throw new Error(`API Request Failed: ${JSON.stringify(res)}`);
  } catch (error) {
    console.error('��� -> file: apis.js:283 -> markAsDelivered -> error:', error);
    throw error;
  }
}

function makeJsonToXml({ housebill, base64 }) {
  return convert({
    'soap12:Envelope': {
      '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
      '@xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
      '@xmlns:soap12': 'http://www.w3.org/2003/05/soap-envelope',
      'soap12:Header': {
        AuthHeader: {
          '@xmlns': 'http://tempuri.org/',
          'UserName': 'biztest',
          'Password': 'Api081020!',
        },
      },
      'soap12:Body': {
        UploadPODDocument: {
          '@xmlns': 'http://tempuri.org/',
          'HAWB': housebill,
          'DocumentDataBase64': base64,
          'DocumentExtension': 'pdf',
        },
      },
    },
  });
}

async function getCurrentLogGroupName({ functionName }) {
  const cloudwatchLogs = new AWS.CloudWatchLogs();
  const logStreamName = await cloudwatchLogs
    .describeLogStreams({
      logGroupName: `/aws/lambda/${functionName}`,
      orderBy: 'LastEventTime',
      descending: true,
      limit: 1,
    })
    .promise();
  console.info(
    '🙂 -> file: index.js:37 -> promises -> logStreamName:',
    JSON.stringify(logStreamName)
  );
  const latestLogStream = _.get(logStreamName, 'logStreams.[0].logStreamName');
  console.info(
    '🙂 -> file: apis.js:332 -> getCurrentLogGroupName -> latestLogStream:',
    latestLogStream
  );
  return latestLogStream;
}

function getLogUrl({ functionName, logStreamName }) {
  const baseUrl = 'https://console.aws.amazon.com/cloudwatch/home';
  const region = `?region=${process.env.REGION}`;
  const logGroupName = `#logsV2:log-groups/log-group/%2Faws%2Flambda%2F${functionName}`;
  const logStream = `/log-events/${encodeURIComponent(logStreamName)}`;
  const encodedUrl = `${baseUrl}${region}${logGroupName}${logStream}`;
  return encodedUrl;
}

async function publishSNSTopicForLocationUpdate({
  housebill,
  callinId,
  message,
  functionName,
  orderId,
}) {
  const subject = `PB WT LOCATION UPDATE ERROR NOTIFICATION - ${ENVIRONMENT} ~ Housebill: ${housebill}`;
  try {
    const logStreamName = await getCurrentLogGroupName({ functionName });
    const logUrl = getLogUrl({ functionName, logStreamName });
    const params = {
      TopicArn: ERROR_SNS_TOPIC_ARN,
      Subject: subject,
      Message: `Function name: ${functionName}\nError Message: ${message}.\nHousebill: ${housebill}\nCallin Id: ${callinId}\nOrder Id: ${orderId}\nTable Name: ${LOCATION_UPDATE_TABLE}\nCheck the table; set the Status to READY to retrigger.\nLatest Log URL: ${logUrl}\nCheck the log for more detailed error.\nPlease check the next log of the mentioned log url.`,
    };
    console.info('🙂 -> file: apis.js:118 -> publishSNSTopic -> params:', params);
    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}

function getAddTrackingNoteXml({ housebill, note }) {
  return convert({
    'soap12:Envelope': {
      '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
      '@xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
      '@xmlns:soap12': 'http://www.w3.org/2003/05/soap-envelope',
      'soap12:Header': {
        AuthHeader: {
          '@xmlns': 'http://tempuri.org/',
          'UserName': WT_SOAP_USERNAME,
          'Password': WT_SOAP_PASSWORD,
        },
      },
      'soap12:Body': {
        WriteTrackingNote: {
          '@xmlns': 'http://tempuri.org/',
          'HandlingStation': '',
          'HouseBill': housebill,
          'TrackingNotes': {
            TrackingNotes: {
              TrackingNoteMessage: note,
            },
          },
        },
      },
    },
  });
}

async function addTrackingNote({ city, state, housebill }) {
  const data = getAddTrackingNoteXml({ housebill, note: `Freight Location: ${city}, ${state}` });
  try {
    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: TRACKING_NOTES_API_URL,
      headers: {
        'Content-Type': 'application/soap+xml; charset=utf-8',
      },
      data,
    };
    console.info('🙂 -> file: apis.js:253 -> markAsDelivered -> config:', config);
    const res = await axios.request(config);

    const shipmentHeaderUpdate = await executePreparedStatement({ city, housebill, state });
    console.info(
      '🙂 -> file: apis.js:396 -> addTrackingNote -> shipmentHeaderUpdate:',
      shipmentHeaderUpdate
    );

    if (_.get(res, 'status', '') === 200) {
      return { response: _.get(res, 'data', ''), payload: data };
    }

    throw new Error(`API Request Failed: ${JSON.stringify(res)}`);
  } catch (error) {
    console.error('��� -> file: apis.js:283 -> markAsDelivered -> error:', error);
    const responseData = _.get(error, 'response.data', _.get(error, 'message'));
    throw new CustomAxiosError(
      `API Request Failed: ${JSON.stringify(responseData)}`,
      responseData,
      data
    );
  }
}

/**
 * Sends the SOAP request to the external service.
 *
 * @param {string} soapRequest - The XML SOAP request payload.
 * @returns {Promise<string>} - The response from the SOAP request.
 */
async function makeSOAPRequest(soapRequest) {
  const config = {
    method: 'post',
    maxBodyLength: Infinity,
    url: process.env.FINALISE_COST_ENDPOINT,
    headers: {
      'Accept': 'text/xml',
      'Content-Type': 'text/xml',
    },
    data: soapRequest,
  };

  try {
    const response = await axios.request(config);
    if (_.get(response, 'status') === 200) {
      return _.get(response, 'data', {});
    }

    throw new Error(`SOAP request failed with status ${response.status}`);
  } catch (error) {
    console.error('Error making SOAP request:', error.message);
    const responseData = _.get(error, 'response.data', _.get(error, 'message'));
    throw new CustomAxiosError(
      `SOAP request failed: ${JSON.stringify(responseData)}`,
      responseData,
      soapRequest
    );
  }
}

module.exports = {
  getOrders,
  checkForPod,
  publishSNSTopic,
  getPODId,
  getPOD,
  uploadPODDoc,
  markAsDelivered,
  uploadPOD,
  publishSNSTopicForLocationUpdate,
  addTrackingNote,
  makeSOAPRequest,
};
