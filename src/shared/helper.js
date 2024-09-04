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
  DB_USERNAME,
  DB_PASSWORD,
  DB_SERVER,
  DB_PORT,
  DB_DATABASE,
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

const config = {
  user: DB_USERNAME,
  password: DB_PASSWORD,
  server: DB_SERVER,
  database: DB_DATABASE,
  port: Number(DB_PORT),
  options: {
    encrypt: true, // for Azure SQL or encryption
    trustServerCertificate: true, // for self-signed certificates
  },
  pool: {
    max: 10, // Max number of connections in the pool
    min: 0, // Minimum number of connections in the pool
    idleTimeoutMillis: 30000, // Close idle connections after 30 seconds
  },
};

// Create a pool
const poolPromise = new sql.ConnectionPool(config)
  .connect()
  .then((pool) => {
    console.info('Connected to MSSQL');
    return pool;
  })
  .catch((err) => {
    console.error('Database Connection Failed!', err);
    throw err;
  });

async function executePreparedStatement({ housebill, city, state }) {
  try {
    // Define configuration for MSSQL connection
    const pool = await poolPromise;
    const ps = new sql.PreparedStatement(pool);
    ps.input('housebill', sql.VarChar);
    ps.input('freight_city', sql.VarChar);
    ps.input('freight_state', sql.VarChar);

    await ps.prepare(
      'UPDATE tbl_ShipmentHeader set FreightCity = @freight_city, FreightState = @freight_state WHERE Housebill = @housebill'
    );
    const result = await ps.execute({ housebill, freight_city: city, freight_state: state });

    await ps.unprepare();
    return result;
  } catch (err) {
    console.error('Prepared Statement error', err);
    throw err;
  }
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
};
