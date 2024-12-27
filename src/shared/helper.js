/*
 * File: src/shared/helper.js
 * Project: PB-WT 214
 * Author: Bizcloud Experts
 * Date: 2024-08-14
 * Confidential and Proprietary
 */

'use strict';

const _ = require('lodash');
const { js2xml } = require('xml-js');
const axios = require('axios');
const AWS = require('aws-sdk');
const moment = require('moment-timezone');
const sql = require('mssql');

const sqs = new AWS.SQS();
const {
  WT_SOAP_USERNAME,
  ADD_MILESTONE_URL_2,
  ADD_MILESTONE_URL,
  ADDRESS_MAPPING_G_API_KEY,
} = process.env;

const types = {
  CONSOL: 'CONSOLE',
  MULTISTOP: 'MULTI-STOP',
  NON_CONSOL: 'NON_CONSOLE',
};

const milestones = {
  BOO: 'BOO',
  APP: 'APP',
  APD: 'APD',
  APL: 'APL',
  TLD: 'TLD',
  COB: 'COB',
  TTC: 'TTC',
  AAD: 'AAD',
  DWP: 'DWP',
  DEL: 'DEL',
  DLA: 'DLA',
  POD: 'POD',
};

const status = {
  PENDING: 'PENDING',
  SENT: 'SENT',
  READY: 'READY',
  FAILED: 'FAILED',
  SKIPPED: 'SKIPPED',
};

function getDynamoUpdateParam(data) {
  const ExpressionAttributeNames = {};
  const ExpressionAttributeValues = {};
  let UpdateExpression = [];
  if (typeof data !== 'object' || !data || Array.isArray(data) || Object.keys(data).length === 0) {
    return { ExpressionAttributeNames, ExpressionAttributeValues, UpdateExpression };
  }

  Object.keys(data).forEach((key) => {
    ExpressionAttributeValues[`:${key}`] = _.get(data, key, '');
    ExpressionAttributeNames[`#${key}`] = key;
    UpdateExpression.push(`#${key} = :${key}`);
  });

  UpdateExpression = UpdateExpression.join(', ');
  UpdateExpression = `set ${UpdateExpression}`;

  return { ExpressionAttributeNames, ExpressionAttributeValues, UpdateExpression };
}

async function generateNonConsolXmlPayload(itemObj1) {
  console.info('🙂 -> file: helper.js:62 -> generateNonConsolXmlPayload -> itemObj1:', itemObj1);
  try {
    const xml = js2xml(
      {
        'soap:Envelope': {
          '_attributes': {
            'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
            'xmlns:soap': 'http://schemas.xmlsoap.org/soap/envelope/',
          },
          'soap:Body': {
            UpdateStatus: {
              _attributes: {
                xmlns: 'http://tempuri.org/', // NOSONAR
              },
              HandlingStation: '',
              HAWB: _.get(itemObj1, 'Housebill', ''),
              UserName: WT_SOAP_USERNAME,
              StatusCode: _.get(itemObj1, 'StatusCode', ''),
              EventDateTime: _.get(itemObj1, 'EventDateTime', ''),
            },
          },
        },
      },
      { compact: true, ignoreComment: true, spaces: 4 }
    );
    console.info('XML payload', xml);
    return xml;
  } catch (error) {
    console.error('Error generating XML:', error);
    throw error;
  }
}

async function generateMultiStopXmlPayload({ consolNo, statusCode, eventDateTime }) {
  try {
    const xml = js2xml(
      {
        'soap:Envelope': {
          '_attributes': {
            'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
            'xmlns:soap': 'http://schemas.xmlsoap.org/soap/envelope/',
          },
          'soap:Body': {
            UpdateStatusByConsolNo: {
              _attributes: {
                xmlns: 'http://tempuri.org/',
              },
              HandlingStation: '',
              ConsolNo: consolNo,
              UserName: WT_SOAP_USERNAME,
              StatusCode: statusCode,
              EventDateTime: eventDateTime,
            },
          },
        },
      },
      { compact: true, ignoreComment: true, spaces: 4 }
    );
    console.info('XML payload', xml);
    return xml;
  } catch (error) {
    console.error('Error generating XML:', error);
    throw error;
  }
}

async function addMilestoneApiDataForConsol(postData) {
  try {
    const config = {
      method: 'post',
      headers: {
        'Accept': 'text/xml',
        'Content-Type': 'text/xml',
      },
      data: postData,
    };

    config.url = `${ADD_MILESTONE_URL_2}?op=UpdateStatus`;

    console.info('config: ', config);
    const res = await axios.request(config);
    if (_.get(res, 'status', '') === 200) {
      return _.get(res, 'data', '');
    }
    throw new Error(`API Request Failed: ${res}`);
  } catch (error) {
    const response = error.response;
    console.error('Error in addMilestoneApi', {
      message: _.get(error, 'message'),
      response: {
        status: response?.status,
        data: response?.data,
      },
    });
    throw error;
  }
}

async function addMilestoneApiDataForNonConsol(postData) {
  try {
    const config = {
      method: 'post',
      headers: {
        'Accept': 'text/xml',
        'Content-Type': 'text/xml',
      },
      data: postData,
    };

    config.url = `${ADD_MILESTONE_URL}`;

    console.info('config: ', config);
    const res = await axios.request(config);
    if (_.get(res, 'status', '') === 200) {
      return _.get(res, 'data', '');
    }
    throw new Error(`API Request Failed: ${res}`);
  } catch (error) {
    const response = _.get(error, 'response');
    console.error('Error in addMilestoneApi', {
      message: _.get(error, 'message'),
      response: {
        status: _.get(response, 'status'),
        data: _.get(response, 'data'),
      },
    });
    throw error;
  }
}

async function deleteMassageFromQueue({ receiptHandle, queueUrl }) {
  try {
    if (!receiptHandle) {
      throw new Error('No receipt handle found in the event');
    }

    // Delete the message from the SQS queue
    const deleteParams = {
      QueueUrl: queueUrl,
      ReceiptHandle: receiptHandle,
    };

    await sqs.deleteMessage({ ...deleteParams }).promise();
    console.info('Message deleted successfully');
  } catch (error) {
    console.error('Error processing SQS event:', error);
  }
}

function getActualTimestamp(timestamp) {
  return (
    moment(Number(timestamp)).format('YYYY-MM-DDTHH:mm:ss') +
    moment(Number(timestamp)).tz('America/Chicago').format('Z')
  );
}

class CustomAxiosError extends Error {
  constructor(message, response, payload) {
    super(message);
    this.name = 'AxiosError';
    this.response = response;
    this.payload = payload;
  }
}

async function executePreparedStatement({ housebill, city, state }) {
  try {
    const url = process.env.DB_URL;
    const apiKey = process.env.DB_API_KEY;

    const config = {
      method: 'post',
      url: url,
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey
      },
      data: {
        query: `UPDATE tbl_ShipmentHeader set FreightCity = '${city}', FreightState = '${state}' WHERE Housebill = '${housebill}'`
      }
    };

    const response = await axios.request(config);

    if (response.status === 200) {
      return response.data;
    }

    throw new Error(`API Request Failed: ${response.status}`);

  } catch (err) {
    console.error('Prepared Statement error', err);
    throw err;
  }
}

/**
 * check address by google api
 * @param {*} address1
 * @param {*} address2
 * @returns
 * NOTE:- need a ssm parameter for google api url
 */
async function checkAddressByGoogleApi(address1, address2) {
  let checkWithGapi = false;
  let partialCheckWithGapi = false;

  try {
    const apiKey = ADDRESS_MAPPING_G_API_KEY;

    // Get geocode data for address1
    const geocode1 = await axios.get(
      `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(
        address1
      )}&key=${apiKey}`
    );
    console.info('geocode1', geocode1);
    if (geocode1.data.status !== 'OK') {
      throw new Error(`Unable to geocode ${address1}`);
    }
    // Get geocode data for address2
    const geocode2 = await axios.get(
      `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(
        address2
      )}&key=${apiKey}`
    );
    console.info('geocode2', geocode2);
    if (geocode2.data.status !== 'OK') {
      throw new Error(`Unable to geocode ${address2}`);
    }
    console.info('geocode1', JSON.stringify(geocode1.data.results));
    console.info('geocode2', JSON.stringify(geocode2.data.results));

    const adType1 = geocode1.data?.results?.[0]?.geometry?.location_type;
    const adType2 = geocode2.data?.results?.[0]?.geometry?.location_type;
    /**
     * check if distance is under 50 meters
     */
    if (adType1 === 'ROOFTOP' && adType2 === 'ROOFTOP') {
      const coords1 = geocode1.data?.results?.[0]?.geometry?.location;
      const coords2 = geocode2.data?.results?.[0]?.geometry?.location;

      // Calculate the distance between the coordinates (in meters)
      checkWithGapi = getDistance(coords1, coords2);
    } else {
      partialCheckWithGapi = true;

      let addressArr = [];
      if (adType1 !== 'ROOFTOP') {
        addressArr = [...addressArr, address1];
      }
      if (adType2 !== 'ROOFTOP') {
        addressArr = [...addressArr, address2];
      }

      throw new Error(
        `Unable to locate address.  Please correct in worldtrak.\n${addressArr.join(' \nand ')}`
      );
    }

    return { checkWithGapi, partialCheckWithGapi };
  } catch (error) {
    console.info('checkAddressByGoogleApi:error', error);
    return { checkWithGapi, partialCheckWithGapi };
  }
}

function getDistance(coords1, coords2) {
  try {
    const earthRadius = 6371000; // Radius of the earth in meters
    const lat1 = toRadians(coords1.lat);
    const lat2 = toRadians(coords2.lat);
    const deltaLat = toRadians(coords2.lat - coords1.lat);
    const deltaLng = toRadians(coords2.lng - coords1.lng);

    const a =
      Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) +
      Math.cos(lat1) * Math.cos(lat2) * Math.sin(deltaLng / 2) * Math.sin(deltaLng / 2);

    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    const distance = earthRadius * c;
    return distance <= 50;
  } catch (error) {
    console.info('error:getDistance', error);
    return false;
  }
}

function toRadians(degrees) {
  return (degrees * Math.PI) / 180;
}

module.exports = {
  types,
  milestones,
  status,
  getDynamoUpdateParam,
  generateNonConsolXmlPayload,
  generateMultiStopXmlPayload,
  addMilestoneApiDataForConsol,
  addMilestoneApiDataForNonConsol,
  deleteMassageFromQueue,
  getActualTimestamp,
  CustomAxiosError,
  executePreparedStatement,
  checkAddressByGoogleApi,
};
