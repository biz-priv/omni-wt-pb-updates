/*
 * File: src/pod-upload/index.js
 * Project: PB-WT 214
 * Author: Bizcloud Experts
 * Date: 2024-08-14
 * Confidential and Proprietary
 */

'use strict';

const _ = require('lodash');
const {
  fetchPendingPOD,
  updateDynamoRow,
  getAparDataByConsole,
  consigneeIsCustomer,
  getShipmentHeaderData,
} = require('../shared/dynamo');
const { getPODId, getPOD, uploadPOD, markAsDelivered, publishSNSTopic } = require('../shared/apis');
const moment = require('moment-timezone');
const { status, types } = require('../shared/helper');

let functionName;
module.exports.handler = async (event, context) => {
  functionName = _.get(context, 'functionName');
  try {
    console.info('🙂 -> file: index.js:6 -> module.exports= -> event:', event);
    const pendingRecords = await fetchPendingPOD();
    console.info('🙂 -> file: index.js:9 -> module.exports= -> pendingRecords:', pendingRecords);
    const promises = pendingRecords.map(async (record) => {
      const dynamoUpdateData = {
        LastUpdatedBy: _.get(context, 'functionName'),
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      };
      let housebill;
      let statusCode;
      let retryCount;
      let type;
      let eventDateTime;
      try {
        const orderId = _.get(record, 'OrderId');
        housebill = _.get(record, 'Housebill');
        console.info('🙂 -> file: index.js:41 -> promises -> housebill:', housebill);
        statusCode = _.get(record, 'StatusCode');
        console.info('🙂 -> file: index.js:43 -> promises -> statusCode:', statusCode);
        retryCount = _.get(record, 'RetryCount');
        console.info('🙂 -> file: index.js:45 -> promises -> retryCount:', retryCount);
        type = _.get(record, 'Type');
        console.info('🙂 -> file: index.js:47 -> promises -> type:', type);
        eventDateTime = _.get(record, 'EventDateTime');
        console.info('🙂 -> file: index.js:49 -> promises -> eventDateTime:', eventDateTime);
        dynamoUpdateData.RetryCount = retryCount + 1;
        const podResponse = await getPODId(orderId);
        console.info('🙂 -> file: index.js:16 -> promises -> podResponse:', podResponse);
        if (!podResponse) {
          dynamoUpdateData.Message = 'POD is not available yet.';
          //* Check for POD until it is available
          // if (dynamoUpdateData.RetryCount >= 25) {
          //   dynamoUpdateData.Status = status.SKIPPED;
          //   dynamoUpdateData.Message = 'POD is not available. Skipping';
          // }
          await updateDynamoRow({ housebill, statusCode, data: dynamoUpdateData });
          return 'POD is not available yet.';
        }
        const imageId = _.get(podResponse, 'id');
        console.info('🙂 -> file: index.js:19 -> promises -> imageId:', imageId);
        const getPODDocResponse = await getPOD(imageId);
        const base64 = Buffer.from(getPODDocResponse).toString('base64');
        if (types.NON_CONSOL === type) {
          const { failedRecords, successRecords } = await uploadPODForNonConsol({
            base64,
            housebill,
            eventDateTime,
          });
          dynamoUpdateData.Message = 'POD Uploaded.';
          dynamoUpdateData.SuccessRecords = successRecords;
          dynamoUpdateData.Status = status.SENT;
          if (failedRecords.length > 0) {
            dynamoUpdateData.FailedRecords = failedRecords;
            dynamoUpdateData.Status = status.FAILED;
            await publishSNSTopic({
              id: housebill,
              status: statusCode,
              functionName,
              message: `Error Details: Error while uploading POD.\nFailed records: ${JSON.stringify(failedRecords)}`,
            });
          }
          return await updateDynamoRow({ housebill, statusCode, data: dynamoUpdateData });
        }
        if ([types.CONSOL, types.MULTISTOP].includes(type)) {
          const { failedRecords, successRecords } = await uploadPODForConsol({
            base64,
            housebill,
            type,
            eventDateTime,
          });
          dynamoUpdateData.Message = 'POD Uploaded.';
          dynamoUpdateData.SuccessRecords = successRecords;
          dynamoUpdateData.Status = status.SENT;
          if (failedRecords.length > 0) {
            dynamoUpdateData.FailedRecords = failedRecords;
            dynamoUpdateData.Status = status.FAILED;
            await publishSNSTopic({
              id: housebill,
              status: statusCode,
              functionName,
              message: `Error Details: Error while uploading POD.\nFailed records: ${JSON.stringify(failedRecords)}`,
            });
          }
          return await updateDynamoRow({ housebill, statusCode, data: dynamoUpdateData });
        }
        return true;
      } catch (error) {
        console.error('Error in getPODId function:', error);
        dynamoUpdateData.Message = _.get(error, 'message');
        dynamoUpdateData.Status = status.FAILED;
        await updateDynamoRow({ housebill, statusCode, data: dynamoUpdateData });
        return await publishSNSTopic({
          id: housebill,
          status: statusCode,
          functionName,
          message: `Error Details:${error}`,
        });
      }
    });
    return await Promise.all(promises);
  } catch (error) {
    console.error('Error in handler function:', error);
    throw error;
  }
};

async function uploadPODForNonConsol({ base64, housebill, eventDateTime }) {
  const successRecords = [];
  const failedRecords = [];
  try {
    const uploadPODDocResponse = await uploadPOD({ base64, housebill });
    console.info(
      '🙂 -> file: index.js:30 -> promises -> uploadPODDocResponse:',
      uploadPODDocResponse
    );
    if (uploadPODDocResponse) {
      console.info('POD uploaded successfully');
      const markAsDelRes = await markAsDelivered(housebill, eventDateTime);
      console.info('🙂 -> file: index.js:35 -> promises -> markAsDelRes:', markAsDelRes);
      successRecords.push({
        Housebill: housebill,
        Status: status.SENT,
        Message: 'POD uploaded successfully',
      });
    }
  } catch (error) {
    failedRecords.push({
      Housebill: housebill,
      ErrorMessage: _.get(error, 'message'),
      Status: status.FAILED,
    });
  }
  return { successRecords, failedRecords };
}

async function uploadPODForConsol({ base64, housebill, type, eventDateTime }) {
  const consolidatedShipments = await getAparDataByConsole({ orderNo: housebill });
  console.info(
    '🙂 -> file: index.js:82 -> uploadPODForConsol -> consolidatedShipments:',
    consolidatedShipments
  );
  const successRecords = [];
  const failedRecords = [];
  for (const shipment of consolidatedShipments) {
    try {
      const conIsCu = await consigneeIsCustomer(_.get(shipment, 'FK_OrderNo'), type);
      if (conIsCu) {
        const shipmentHeaderData = await getShipmentHeaderData({
          orderNo: _.get(shipment, 'FK_OrderNo'),
        });
        const housebillForShipment = _.get(shipmentHeaderData, '[0].Housebill');
        console.info('🙂 -> file: index.js:115 -> housebillForShipment:', housebillForShipment);
        const uploadPODDocResponse = await uploadPOD({
          base64,
          housebill: housebillForShipment,
        });
        console.info(
          '🙂 -> file: index.js:30 -> promises -> uploadPODDocResponse:',
          uploadPODDocResponse
        );
        if (uploadPODDocResponse) {
          console.info('POD uploaded successfully');
          const markAsDelRes = await markAsDelivered(housebillForShipment, eventDateTime);
          console.info('🙂 -> file: index.js:35 -> promises -> markAsDelRes:', markAsDelRes);
          successRecords.push({
            ConsolNo: housebill,
            FK_OrderNo: _.get(shipment, 'FK_OrderNo'),
            Status: status.SENT,
            Message: 'POD uploaded successfully',
          });
        } else {
          failedRecords.push({
            ConsolNo: housebill,
            FK_OrderNo: _.get(shipment, 'FK_OrderNo'),
            Status: status.FAILED,
            Message: 'POD upload failed.',
          });
        }
      }
    } catch (error) {
      console.error('🙂 -> file: index.js:110 -> uploadPODForConsol -> error:', error);
      failedRecords.push({
        ConsolNo: housebill,
        FK_OrderNo: _.get(shipment, 'FK_OrderNo'),
        ErrorMessage: _.get(error, 'message'),
        Statue: status.FAILED,
      });
    }
  }
  return { successRecords, failedRecords };
}
