/*
 * File: src/finalise-cost/index.js
 * Project: PB-WT Finalize Cost
 * Author: Bizcloud Experts
 * Date: 2024-10-08
 * Confidential and Proprietary
 */

'use strict';

const _ = require('lodash');
const { makeSOAPRequest, updateAsComplete } = require('../shared/apis');
const {
  deleteMassageFromQueue,
  status,
  types,
  sendSESEmail,
  generateFinaliseCostPayload,
  generateEmailContent,
  parseRecord,
} = require('../shared/helper');
const {
  getShipmentDetails,
  storeFinalizeCostStatus,
  isAlreadyProcessed,
  queryUsersTable,
} = require('../shared/dynamo');
const xmljs = require('xml-js');
const moment = require('moment-timezone');

const { FINALISE_COST_QUEUE_URL, STAGE } = process.env;

/**
 * Main Lambda handler for processing the event.
 * @param {Object} event - The AWS Lambda event object
 * @returns {Promise<boolean>} - Returns true if processing was successful
 */
module.exports.handler = async (event) => {
  console.info('Event received:', JSON.stringify(event, null, 2));

  try {
    const records = _.get(event, 'Records', []);
    if (_.isEmpty(records)) {
      console.info('No records found in the event.');
      return false;
    }

    const results = await Promise.all(records.map(processRecord));
    return results.some((result) => result === true);
  } catch (error) {
    console.error('Error in handler:', error);
    return false;
  }
};

/**
 * Process a single record from the event
 * @param {Object} record - The record to process
 * @returns {Promise<boolean>} - Returns true if processing was successful
 */
async function processRecord(record) {
  const receiptHandle = _.get(record, 'receiptHandle', '');
  let shipmentId = null;
  let shipmentDetails = {};
  let type = '';
  try {
    const parsedRecord = parseRecord(record);
    if (!parsedRecord) return false;

    shipmentId = _.get(parsedRecord, 'id', '');
    shipmentDetails = await getShipmentDetails({ shipmentId });

    if (!isValidShipment(shipmentDetails)) {
      console.info('Invalid shipment. Skipping.');
      return false;
    }

    const { housebill, orderNo, consolNo } = extractShipmentInfo(shipmentDetails);
    console.info('🚀 ~ file: index.js:74 ~ processRecord ~ consolNo:', consolNo);
    console.info('🚀 ~ file: index.js:74 ~ processRecord ~ orderNo:', orderNo);
    console.info('🚀 ~ file: index.js:74 ~ processRecord ~ housebill:', housebill);

    // Check if the record is already processed
    const processedRecords = await isAlreadyProcessed(shipmentId);
    console.info('🚀 ~ file: index.js:84 ~ processRecord ~ processedRecords:', processedRecords);

    // If the record has a Status of SENT, send an error email and return
    const sentRecord = processedRecords.find((rec) => rec.Status === status.SENT);
    if (sentRecord) {
      console.info(`Record with shipmentId ${shipmentId} is already SENT. Skipping processing.`);
      // query users table with newImages attrubute billing_user_id and query for id = billing_user_id. Projection should be email

      const userId = _.get(parsedRecord, 'billing_user_id', 'NA');
      const billingUser = await queryUsersTable({ userId });
      console.info('🚀 ~ file: index.js:93 ~ processRecord ~ billingUser:', billingUser);
      const userEmail = _.get(billingUser, '[0].email_address', '');
      console.info('🚀 ~ file: index.js:95 ~ processRecord ~ userEmail:', userEmail);

      const emailContent = generateEmailContent({
        shipmentId,
        orderNo,
        consolNo,
        housebill,
        errorDetails: 'This shipment has already been finalized.',
        type,
      });
      await sendSESEmail({
        message: emailContent,
        subject: `Finalize Cost Failed for Shipment ${shipmentId} - ${_.toUpper(STAGE)}`,
        userEmail,
      });
      return false;
    }

    if (!isReadyToBill(parsedRecord)) {
      console.info('Record not ready to bill.');
      return false;
    }

    const totalCharges = _.get(parsedRecord, 'total_charge', '0');

    type = _.get(shipmentDetails, 'Type', 'NA');
    console.info('🚀 ~ file: index.js:83 ~ processRecord ~ type:', type);

    const result = await processFinalizedCost(shipmentId, totalCharges, type, {
      housebill,
      orderNo,
      consolNo,
    });

    return result;
  } catch (error) {
    console.error('Error processing record:', error);
    await handleProcessingError(error, shipmentId, shipmentDetails, type);
    await deleteMassageFromQueue({
      queueUrl: FINALISE_COST_QUEUE_URL,
      receiptHandle,
    });
    return false;
  }
}

/**
 * Check if a shipment is valid
 * @param {Object} shipmentDetails - The shipment details to validate
 * @returns {boolean} - Returns true if the shipment is valid
 */
function isValidShipment(shipmentDetails) {
  const type = _.get(shipmentDetails, 'Type');
  const housebill = _.get(shipmentDetails, 'housebill');
  return type && shipmentDetails && housebill;
}

/**
 * Extract relevant information from shipment details
 * @param {Object} shipmentDetails - The shipment details
 * @returns {Object} - Object containing housebill, orderNo, and consolNo
 */
function extractShipmentInfo(shipmentDetails) {
  const type = _.get(shipmentDetails, 'Type');
  let housebill;
  let orderNo;
  let consolNo;

  switch (type) {
    case types.NON_CONSOL:
      orderNo = _.get(shipmentDetails, 'FK_OrderNo');
      housebill = _.get(shipmentDetails, 'Housebill');
      consolNo = _.get(shipmentDetails, 'consolNo', '0');
      break;
    case types.CONSOL:
      orderNo = _.get(shipmentDetails, 'FK_OrderNo', '0');
      housebill = _.get(shipmentDetails, 'Housebill', '0');
      consolNo = _.get(shipmentDetails, 'FK_OrderNo', '0');
      break;
    case types.MULTISTOP:
      orderNo = _.get(shipmentDetails, 'FK_OrderNo', '0');
      housebill = _.get(shipmentDetails, 'Housebill', '0');
      consolNo = _.get(shipmentDetails, 'ConsolNo', '0');
      break;
    default:
      orderNo = '0';
      housebill = '0';
      consolNo = '0';
  }

  return { housebill, orderNo, consolNo };
}

/**
 * Check if a record is ready to bill
 * @param {Object} record - The record to check
 * @returns {boolean} - Returns true if the record is ready to bill
 */
function isReadyToBill(record) {
  return _.get(record, 'ready_to_bill', 'N') === 'Y';
}

/**
 * Process the finalized cost
 * @param {string} shipmentId - The shipment ID
 * @param {string} totalCharges - The total charges
 * @param {Object} shipmentInfo - Object containing shipment information
 * @returns {Promise<boolean>} - Returns true if processing was successful
 */
async function processFinalizedCost(shipmentId, totalCharges, type, shipmentInfo) {
  const finaliseCostRequest = generateFinaliseCostPayload({
    referenceNo: shipmentId,
    totalCharges,
    invoiceNumber: shipmentId,
    invoiceDate: moment.tz('America/Chicago').format('YYYY-MM-DD'),
  });

  const response = await makeSOAPRequest(finaliseCostRequest);
  const errorDetails = parseSOAPResponse(response);
  console.info('🚀 ~ file: index.js:176 ~ processFinalizedCost ~ errorMessage:', errorDetails);
  const errorMessage = _.get(errorDetails, 'message', '');

  if (_.get(errorDetails, 'status', false) === false) {
    let customErrorMessage;

    if (errorMessage.includes('Reference # for Vendor Not Found')) {
      customErrorMessage = `The reference number ${shipmentId} for LIVELOGI could not be found.`;
    } else if (
      errorMessage.includes('The Vendor/Reference # combination Combination Must be Unique')
    ) {
      customErrorMessage =
        'The combination of vendor and reference number must be unique. Duplicate records detected.';
    } else if (errorMessage.endsWith('is already Finalized')) {
      customErrorMessage = `This invoice/refNo with ${shipmentId} has already been finalized. This request could not be processed again.`;
    } else {
      customErrorMessage = errorMessage;
    }

    await storeFinalizeCostStatus({
      shipmentInfo,
      shipmentId,
      finaliseCostRequest,
      response,
      Status: status.FAILED,
      errorMessage: customErrorMessage,
      type,
    });
    throw new Error(`${customErrorMessage}`);
  } else if (errorMessage.includes('Pending Approval')) {
    console.info('Error message indicates Pending Approval. Sending email.');

    // Generate email content
    const emailContent = generateEmailContent({
      shipmentId,
      orderNo: _.get(shipmentInfo, 'orderNo', ''),
      consolNo: _.get(shipmentInfo, 'consolNo', ''),
      housebill: _.get(shipmentInfo, 'housebill', ''),
      errorDetails: 'Due to discrepancies in cost, this shipment is in a pending approval state.',
      type,
    });

    await sendSESEmail({
      message: emailContent,
      subject: `Shipment with #PRO Number ${shipmentId} is Pending Approval`,
    });

    console.info('Pending Approval email sent.');
  } else if (
    _.get(errorDetails, 'status', false) === true &&
    !errorMessage.includes('Pending Approval')
  ) {
    console.info('Shipment should be marked as complete.');
    let fkOrderNo;
    if (type === types.MULTISTOP) {
      fkOrderNo = _.get(shipmentInfo, 'consolNo');
    } else {
      fkOrderNo = _.get(shipmentInfo, 'orderNo');
    }
    const query = `update dbo.tbl_shipmentapar set Complete = 'Y' where fk_orderno='${fkOrderNo}' and APARCode = 'V'`;
    await updateAsComplete(query);
  }

  await storeFinalizeCostStatus({
    shipmentInfo,
    shipmentId,
    finaliseCostRequest,
    response,
    Status: status.SENT,
    errorMessage,
    type,
  });

  return true;
}

/**
 * Handle processing errors
 * @param {Error} error - The error object
 * @param {string} shipmentId - The shipment ID
 * @param {Object} shipmentInfo - Object containing shipment information
 */
async function handleProcessingError(error, shipmentId, shipmentInfo, type) {
  const { housebill, orderNo, consolNo } = extractShipmentInfo(shipmentInfo);
  await sendErrorNotification(shipmentId, { housebill, orderNo, consolNo }, error.message, type);
}

/**
 * Parses the SOAP response to determine success or failure.
 * @param {string} xmlResponse - The XML response from the SOAP request
 * @returns {string|null} - An error message if there is any, otherwise null
 */
function parseSOAPResponse(xmlResponse) {
  try {
    const result = xmljs.xml2js(xmlResponse, { compact: true });
    const finalizeCostsResult = _.get(
      result,
      'soap:Envelope.soap:Body.FinalizeCostsResponse.FinalizeCostsResult',
      {}
    );

    const success = _.get(finalizeCostsResult, 'Success._text', 'false') === 'true';
    return {
      status: success,
      message: _.get(finalizeCostsResult, 'ErrorMessage._text', 'Unknown error'),
    };
  } catch (error) {
    console.error('Error parsing SOAP response:', error);
    throw error;
  }
}

/**
 * Send error notification via email
 * @param {string} shipmentId - The shipment ID
 * @param {Object} shipmentInfo - Object containing shipment information
 * @param {string} errorDetails - Error details to include in the email
 */
async function sendErrorNotification(shipmentId, shipmentInfo, errorDetails, type) {
  const { orderNo, consolNo, housebill } = shipmentInfo;
  const subject = `${_.toUpper(STAGE)} - Failed to Finalise Cost for #PRO Number: ${shipmentId}`;
  const message = generateEmailContent({
    shipmentId,
    orderNo,
    consolNo,
    housebill,
    errorDetails,
    type,
  });

  await sendSESEmail({ message, subject });
}
