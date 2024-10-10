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

const ses = new AWS.SES();
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
  OMNI_NO_REPLY_EMAIL,
  WT_SOAP_PASSWORD,
  LIVELOGI_VENDOR_REMITNO,
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
    // Define configuration for MSSQL connection
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

const EMAIL_RECIPIENTS = ['msazeed@omnilogistics.com', 'juddin@omnilogistics.com'];

/**
 * Send email using AWS SES
 * @param {Object} params - Parameters for sending email
 */
async function sendSESEmail({ message, subject }) {
  try {
    const params = {
      Destination: {
        ToAddresses: EMAIL_RECIPIENTS,
      },
      Message: {
        Body: {
          Html: {
            Data: message,
            Charset: 'UTF-8',
          },
        },
        Subject: {
          Data: subject,
          Charset: 'UTF-8',
        },
      },
      Source: OMNI_NO_REPLY_EMAIL,
    };

    await ses.sendEmail(params).promise();
  } catch (error) {
    console.error('Error sending email with SES:', error);
    throw error;
  }
}

/**
 * Generates the payload for the SOAP request.
 * @param {Object} params - Parameters for the SOAP request
 * @returns {string} - The generated SOAP request
 */
function generateFinaliseCostPayload({ referenceNo, totalCharges, invoiceNumber, invoiceDate }) {
  if (
    _.isEmpty(referenceNo) ||
    _.isEmpty(totalCharges) ||
    _.isEmpty(invoiceNumber) ||
    _.isEmpty(invoiceDate)
  ) {
    throw new Error('All parameters are required and must not be empty');
  }

  return `<?xml version="1.0" encoding="utf-8"?>
  <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
    <soap12:Header>
      <AuthHeader xmlns="http://tempuri.org/">
        <UserName>${WT_SOAP_USERNAME}</UserName>
        <Password>${WT_SOAP_PASSWORD}</Password>
      </AuthHeader>
    </soap12:Header>
    <soap12:Body>
      <FinalizeCosts xmlns="http://tempuri.org/">
        <RemitVendorNo>${LIVELOGI_VENDOR_REMITNO}</RemitVendorNo>
        <ReferenceNo>${referenceNo}</ReferenceNo>
        <TotalCharges>${totalCharges}</TotalCharges>
        <InvoiceNumber>${invoiceNumber}</InvoiceNumber>
        <InvoiceDate>${invoiceDate}</InvoiceDate>
      </FinalizeCosts>
    </soap12:Body>
  </soap12:Envelope>`;
}

/**
 * Generate HTML email content
 * @param {Object} params - Parameters for email content
 * @returns {string} - HTML email content
 */
function generateEmailContent({
  shipmentId,
  orderNo,
  consolNo,
  housebill,
  errorDetails = '',
  type,
}) {
  return `<!DOCTYPE html>
    <html>
    <head>
      <style>
        body { font-family: Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 0; }
        .container { padding: 20px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; background-color: #fff; max-width: 600px; }
        .highlight { font-weight: bold; }
        .footer { font-size: 0.85em; color: #888; margin-top: 20px; }
        .contact-support { margin-top: 15px; color: #444; font-size: 1em; }
      </style>
    </head>
    <body>
      <div class="container">
        <p>Dear Team,</p>
        <p>We were unable to finalize the cost associated with the shipment. Below are the details:</p>
        <p>
          <span class="highlight">#PRO:</span> <strong>${shipmentId}</strong><br>
          ${type !== types.MULTISTOP ? `<span class="highlight">FileNo:</span> <strong>${orderNo}</strong><br>` : ''}
          <span class="highlight">Consolidation Number:</span> <strong>${consolNo}</strong><br>
          <span class="highlight">Housebill:</span> <strong>${housebill}</strong>
          ${errorDetails ? `<br><span class="highlight">Error Details:</span> ${errorDetails}` : ''}
        </p>
        <p>Thank you,<br>Omni Automation System</p>
        <p class="footer">Note: This is a system generated email. Please do not reply to this email.</p>
      </div>
    </body>
    </html>`;
}

/**
 * Parse the incoming record
 * @param {Object} record - The record to parse
 * @returns {Object|null} - The parsed record or null if invalid
 */
function parseRecord(record) {
  const body = _.get(record, 'body', '');
  const { Message } = JSON.parse(body);
  const parsedMessage = JSON.parse(Message);
  return AWS.DynamoDB.Converter.unmarshall(_.get(parsedMessage, 'dynamodb.NewImage', {}));
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
  sendSESEmail,
  generateFinaliseCostPayload,
  generateEmailContent,
  parseRecord,
};
