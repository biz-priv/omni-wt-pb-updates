/*
 * File: src/finalise-cost/index.js
 * Project: PB-WT Finalize Cost
 * Author: Bizcloud Experts
 * Date: 2024-10-08
 * Confidential and Proprietary
 */

'use strict';
const AWS = require('aws-sdk');
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
  getControlTowerEmail,
} = require('../shared/helper');
const {
  getShipmentDetails,
  storeFinalizeCostStatus,
  isAlreadyProcessed,
  queryUsersTable,
  queryChargesTable,
  queryShipmentAparTable,
  getStationCode,
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
  let userId = '';
  let wtCtEmail = '';
  try {
    // Validate record transition before proceeding
    if (!isTransitionToReadyToBill(record)) {
      console.info('Record does not meet the ready_to_bill transition criteria. Skipping...');
      return false;
    }

    const parsedRecord = parseRecord(record);
    if (!parsedRecord) return false;

    shipmentId = _.get(parsedRecord, 'id', '');
    shipmentDetails = await getShipmentDetails({ shipmentId });

    if (!isValidShipment(shipmentDetails)) {
      console.info('Invalid shipment. Skipping.');
      return false;
    }
    userId = _.get(parsedRecord, 'billing_user_id', 'NA');
    type = _.get(shipmentDetails, 'Type', 'NA');
    console.info('🚀 ~ file: index.js:83 ~ processRecord ~ type:', type);
    const { housebill, orderNo, consolNo } = extractShipmentInfo(shipmentDetails);
    console.info('🚀 ~ file: index.js:79 ~ processRecord ~ consolNo:', consolNo);
    console.info('🚀 ~ file: index.js:80 ~ processRecord ~ orderNo:', orderNo);
    console.info('🚀 ~ file: index.js:81 ~ processRecord ~ housebill:', housebill);

    if (!isReadyToBill(parsedRecord)) {
      console.info('Record not ready to bill.');
      return false;
    }
    // Check if the record is already processed
    const processedRecords = await isAlreadyProcessed(shipmentId);
    console.info('🚀 ~ file: index.js:85 ~ processRecord ~ processedRecords:', processedRecords);

    const totalCharges = _.get(parsedRecord, 'total_charge', '0');
    const otherCharges = _.get(parsedRecord, 'otherchargetotal', '0.0');
    const freightCharges = _.get(parsedRecord, 'freight_charge', '0.0');
    console.info('🚀 ~ file: index.js:138 ~ processRecord ~ otherCharges:', otherCharges);
    // retrieve station code
    const stationCode = await getStationCode(orderNo, type, consolNo);
    console.info('🚀 ~ file: index.js:140 ~ processRecord ~ stationCode:', stationCode);

    // retrieve control tower email
    wtCtEmail = getControlTowerEmail(stationCode);

    const billingUser = await queryUsersTable({ userId });
    console.info('🚀 ~ file: index.js:93 ~ processRecord ~ billingUser:', billingUser);

    // retrieve PB User email
    const pbUserEmail = _.get(billingUser, '[0].email_address', '');
    console.info('🚀 ~ file: index.js:95 ~ processRecord ~ userEmail:', pbUserEmail);

    const userEmails = [pbUserEmail, wtCtEmail]
      .filter((email) => email) // Remove empty entries
      .join(',');
    // If the record has a Status of SENT, send an error email and return
    const sentRecord = processedRecords.find((rec) => rec.Status === status.SENT);
    if (sentRecord) {
      console.info(`Record with shipmentId ${shipmentId} is already SENT. Skipping processing.`);

      let liveCharges = [];

      if (otherCharges !== '0.0') {
        console.info('Other charges are greater than 0.');
        liveCharges = await queryChargesTable({ shipmentId });
        console.info('🚀 ~ file: index.js:304 ~ handlePendingApproval ~ liveCharges:', liveCharges);
      }
      const emailContent = generateEmailContent({
        shipmentId,
        orderNo,
        consolNo,
        housebill,
        errorDetails: 'This shipment has already been finalized.',
        type,
        liveCharges,
        totalCharges,
        freightCharges,
      });

      await sendSESEmail({
        message: emailContent,
        subject: `Finalize Cost Failed for Shipment ${shipmentId} - ${_.toUpper(STAGE)}`,
        userEmail: userEmails,
      });
      return false;
    }

    const result = await processFinalizedCost(
      shipmentId,
      totalCharges,
      type,
      {
        housebill,
        orderNo,
        consolNo,
      },
      otherCharges,
      freightCharges,
      userEmails
    );

    return result;
  } catch (error) {
    console.error('Error processing record:', error);
    await handleProcessingError(error, shipmentId, shipmentDetails, type, userId, wtCtEmail);
    await deleteMassageFromQueue({
      queueUrl: FINALISE_COST_QUEUE_URL,
      receiptHandle,
    });
    return false;
  }
}

/**
 * Check if the record transition is ready to process
 * @param {Object} record - The DynamoDB record
 * @returns {boolean} - Returns true if the record meets the condition
 */
function isTransitionToReadyToBill(record) {
  const body = _.get(record, 'body', '');
  const { Message } = JSON.parse(body);
  const parsedMessage = JSON.parse(Message);
  const oldImage = AWS.DynamoDB.Converter.unmarshall(_.get(parsedMessage, 'dynamodb.OldImage', {}));
  const newImage = AWS.DynamoDB.Converter.unmarshall(_.get(parsedMessage, 'dynamodb.NewImage', {}));

  return _.get(oldImage, 'ready_to_bill') === 'N' && _.get(newImage, 'ready_to_bill') === 'Y';
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
 * @param {string} type - The shipment type
 * @param {Object} shipmentInfo - Object containing shipment information
 * @returns {Promise<boolean>} - Returns true if processing was successful
 */
async function processFinalizedCost(
  shipmentId,
  totalCharges,
  type,
  shipmentInfo,
  otherCharges,
  freightCharges,
  userEmails
) {
  try {
    const finaliseCostRequest = generateFinaliseCostPayload({
      referenceNo: shipmentId,
      totalCharges,
      invoiceNumber: shipmentId,
      invoiceDate: moment.tz('America/Chicago').format('YYYY-MM-DD'),
    });

    const response = await makeSOAPRequest(finaliseCostRequest);
    const errorDetails = parseSOAPResponse(response);
    const errorMessage = _.get(errorDetails, 'message', '');
    const errorStatus = _.get(errorDetails, 'status', false);

    console.info('Error Details:', errorDetails);

    if (!errorStatus) {
      await handleError(
        shipmentId,
        errorMessage,
        shipmentInfo,
        finaliseCostRequest,
        response,
        type
      );
    } else if (errorMessage.includes('Pending Approval')) {
      await handlePendingApproval(
        shipmentId,
        shipmentInfo,
        type,
        totalCharges,
        otherCharges,
        freightCharges,
        userEmails
      );
    } else {
      await markShipmentAsComplete(shipmentInfo, type, shipmentId);
    }

    await storeFinalizeCostStatus({
      shipmentInfo,
      shipmentId,
      finaliseCostRequest,
      response,
      Status: errorStatus ? status.SENT : status.FAILED,
      errorMessage,
      type,
    });

    return true;
  } catch (error) {
    console.error('Error in processFinalizedCost:', error);
    throw error;
  }
}

/**
 * Handle errors during processing
 * @param {string} shipmentId
 * @param {string} errorMessage
 * @param {Object} shipmentInfo
 * @param {Object} finaliseCostRequest
 * @param {Object} response
 * @param {string} type
 */
async function handleError(
  shipmentId,
  errorMessage,
  shipmentInfo,
  finaliseCostRequest,
  response,
  type
) {
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

  throw new Error(customErrorMessage);
}

/**
 * Handle shipments in pending approval state
 * @param {string} shipmentId
 * @param {Object} shipmentInfo
 * @param {string} type
 */
/**
 * Handle shipments in pending approval state
 * @param {string} shipmentId
 * @param {Object} shipmentInfo
 * @param {string} type
 */
async function handlePendingApproval(
  shipmentId,
  shipmentInfo,
  type,
  totalCharges,
  otherCharges,
  freightCharges,
  userEmails
) {
  let liveCharges = [];

  if (otherCharges !== '0.0') {
    console.info('Other charges are greater than 0.');
    liveCharges = await queryChargesTable({ shipmentId });
    console.info('🚀 ~ file: index.js:304 ~ handlePendingApproval ~ liveCharges:', liveCharges);
  }

  const emailContent = generateEmailContent({
    shipmentId,
    orderNo: _.get(shipmentInfo, 'orderNo', ''),
    consolNo: _.get(shipmentInfo, 'consolNo', ''),
    housebill: _.get(shipmentInfo, 'housebill', ''),
    type,
    liveCharges,
    totalCharges,
    freightCharges,
  });
  // Convert userEmails string back to an array
  const emailList = userEmails.split(',');

  // Add the omnicosting email to the array
  emailList.push('omnicosting@omnilogistics.com');

  // Join the array back into a string for the SES email function
  const finalUserEmails = emailList.join(',');

  await sendSESEmail({
    message: emailContent,
    subject: `Shipment with #PRO Number ${shipmentId} is Pending Approval - ${_.toUpper(STAGE)}`,
    userEmail: finalUserEmails,
  });

  await markShipmentAsComplete(shipmentInfo, type, shipmentId);
  console.info('Pending Approval email sent. Shipment marked as complete.');
}

/**
 * Mark the shipment as complete in the database
 * @param {Object} shipmentInfo
 * @param {string} type
 */
async function markShipmentAsComplete(shipmentInfo, type, shipmentId) {
  try {
    let fkOrderNo;
    let fkOrderNos = [];
    let consolNo;

    if (type === types.MULTISTOP || type === types.CONSOL) {
      consolNo = _.get(shipmentInfo, 'consolNo');
      const results = await queryShipmentAparTable(consolNo); // Query the table using ConsolNo
      console.info('🚀 ~ file: index.js:380 ~ markShipmentAsComplete ~ results:', results);
      fkOrderNos = results.map((item) => item.FK_OrderNo); // Extract FK_OrderNo from the results
    } else {
      fkOrderNo = _.get(shipmentInfo, 'orderNo');
      if (fkOrderNo) fkOrderNos.push(fkOrderNo);
    }

    // Mark each FK_OrderNo as complete
    for (const orderNo of fkOrderNos) {
      const query = `UPDATE dbo.tbl_shipmentapar SET Complete = 'Y' WHERE fk_orderno='${orderNo}' AND APARCode = 'V' AND RefNo = '${shipmentId}'`;
      await updateAsComplete(query);
    }

    if (consolNo) {
      const query = `UPDATE dbo.tbl_shipmentapar SET Complete = 'Y' WHERE fk_orderno='${consolNo}' AND APARCode = 'V' AND RefNo = '${shipmentId}'`;
      await updateAsComplete(query);
    }

    console.info('All relevant shipments are marked as complete.');
  } catch (error) {
    console.error('Error marking shipment as complete:', error);
    throw error;
  }
}

/**
 * Handle processing errors
 * @param {Error} error - The error object
 * @param {string} shipmentId - The shipment ID
 * @param {Object} shipmentInfo - Object containing shipment information
 */
async function handleProcessingError(error, shipmentId, shipmentInfo, type, userId, wtCtEmail) {
  const { housebill, orderNo, consolNo } = extractShipmentInfo(shipmentInfo);
  await sendErrorNotification(
    shipmentId,
    { housebill, orderNo, consolNo },
    error.message,
    type,
    userId,
    wtCtEmail
  );
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
 * @param {string} type - Type of the shipment (e.g., MULTISTOP)
 * @param {string} userId - User ID for fetching billing user details
 */
async function sendErrorNotification(
  shipmentId,
  shipmentInfo,
  errorDetails,
  type,
  userId,
  wtCtEmail
) {
  const { orderNo, consolNo, housebill } = shipmentInfo;

  // Format subject line
  const subject = `${_.toUpper(STAGE)} - Failed to Finalize Cost for #PRO Number: ${shipmentId}`;

  // Generate email content
  const message = generateEmailContent({
    shipmentId,
    orderNo,
    consolNo,
    housebill,
    errorDetails,
    type,
  });

  let pbUserEmail = '';
  // Fetch email addresses if error involves reference number
  if (errorDetails.includes('The reference number')) {
    try {
      const billingUser = await queryUsersTable({ userId });
      pbUserEmail = _.get(billingUser, '[0].email_address', '');
      console.info('Billing User Email:', pbUserEmail);
    } catch (error) {
      console.error('Error fetching user emails:', error);
    }
  }

  // Combine emails and remove invalid/empty entries
  const userEmails = [pbUserEmail, wtCtEmail]
    .filter((email) => email) // Remove empty entries
    .join(',');

  // Send email
  try {
    await sendSESEmail({ message, subject, userEmail: userEmails });
    console.info('Error notification email sent successfully.');
  } catch (error) {
    console.error('Error sending email notification:', error);
    throw error;
  }
}
