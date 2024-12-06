/*
 * File: src/add-milestone/index.js
 * Project: PB-WT 214
 * Author: Bizcloud Experts
 * Date: 2024-08-14
 * Confidential and Proprietary
 */

'use strict';

const AWS = require('aws-sdk');
const _ = require('lodash');
const {
  consigneeIsCustomer,
  getAparDataByConsole,
  getShipmentHeaderData,
  getShipmentForStop,
  updateMilestone,
  getTotalStop,
  updateStatusTable,
  updateDynamoRow,
} = require('../shared/dynamo');
const moment = require('moment-timezone');
const {
  types,
  milestones,
  status,
  generateNonConsolXmlPayload,
  generateMultiStopXmlPayload,
  addMilestoneApiDataForConsol,
  addMilestoneApiDataForNonConsol,
} = require('../shared/helper');
const { publishSNSTopic } = require('../shared/apis');

let functionName;

module.exports.handler = async (event, context) => {
  console.info(
    '🙂 -> file: index.js:30 -> module.exports.handler= -> event:',
    JSON.stringify(event)
  );

  functionName = _.get(context, 'functionName');
  const records = _.get(event, 'Records', []);

  const promises = records.map(async (record) => {
    let XMLpayLoad;
    let dataResponse;
    let OrderId;
    let Housebill;
    let EventDateTime;
    let StatusCode;
    try {
      const newUnMarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

      OrderId = _.get(newUnMarshalledRecord, 'OrderId');
      console.info('🙂 -> file: index.js:44 -> promises -> OrderId:', OrderId);
      Housebill = _.get(newUnMarshalledRecord, 'Housebill');
      console.info('🙂 -> file: index.js:46 -> promises -> Housebill:', Housebill);
      EventDateTime = _.get(newUnMarshalledRecord, 'EventDateTime');
      console.info('🙂 -> file: index.js:48 -> promises -> EventDateTime:', EventDateTime);
      StatusCode = _.get(newUnMarshalledRecord, 'StatusCode');
      console.info('🙂 -> file: index.js:50 -> promises -> StatusCode:', StatusCode);

      const eventTime = EventDateTime;
      console.info('🙂 -> file: index.js:53 -> promises -> eventTime:', eventTime);
      const type = _.get(newUnMarshalledRecord, 'Type', '');
      console.info('🙂 -> file: index.js:55 -> promises -> type:', type);
      const originalStatus = _.get(newUnMarshalledRecord, 'StatusCode');
      console.info('🙂 -> file: index.js:76 -> promises -> originalStatus:', originalStatus);

      if (type === types.NON_CONSOL) {
        //* Process the update for non console shipment.
        return await processForNonConsol({
          originalStatus,
          housebill: Housebill,
          orderId: OrderId,
          eventTime,
        });
      }
      if (type === types.CONSOL) {
        //* Process the update for console shipment.
        return await processForConsol({
          originalStatus,
          type,
          orderNo: Housebill,
          orderId: OrderId,
          eventTime,
        });
      }

      if (type === types.MULTISTOP) {
        return await processForMultiStop({
          orderId: OrderId,
          orderNo: Housebill,
          originalStatus,
          eventTime,
        });
      }
    } catch (error) {
      console.error('Error processing event:', error);
      await updateStatusTable(
        Housebill,
        StatusCode,
        status.FAILED,
        XMLpayLoad,
        dataResponse,
        error.message
      );
      await publishSNSTopic({
        id: Housebill,
        status: StatusCode,
        message: `Error Details:${error}`,
        functionName,
      });
      throw error;
    }
    return true;
  });

  return await Promise.all(promises);
};

async function processForNonConsol({ originalStatus, orderId, housebill, eventTime }) {
  let XMLpayLoad;
  let dataResponse;
  try {
    //* get the xml payload for WT api call
    XMLpayLoad = await generateNonConsolXmlPayload({
      Housebill: housebill,
      StatusCode: originalStatus,
      EventDateTime: eventTime,
    });
    console.info('XML Payload Generated :', XMLpayLoad);

    //* Make the WT api call
    dataResponse = await addMilestoneApiDataForNonConsol(XMLpayLoad);
    console.info('dataResponse', dataResponse);

    //* If the milestone is DWP (Delivered without POD) or DEL (Delivered),
    //* insert a record to the status table to check the POD availability.
    if ([milestones.DWP, milestones.DEL].includes(originalStatus)) {
      const insertParam = {
        OrderId: orderId,
        EventDateTime: eventTime,
        Housebill: housebill,
        StatusCode: milestones.POD,
        Status: status.PENDING,
        Type: types.NON_CONSOL,
        RetryCount: 0,
        LastUpdatedBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      };
      await updateMilestone(insertParam);
    }

    //* Update the status for the milestone to SUCCESS after successfully updating it to WT
    const data = {
      Status: status.SENT,
      Payload: XMLpayLoad,
      Response: dataResponse,
      LastUpdatedBy: functionName,
      LastUpdatedAt: moment.tz('America/Chicago').format(),
    };
    return await updateDynamoRow({ housebill, statusCode: originalStatus, data });
  } catch (error) {
    console.error('🙂 -> file: index.js:299 -> processForNonConsol -> error:', error);

    //* Update the record to FAILED if anything fails.
    const data = {
      Status: status.FAILED,
      Payload: XMLpayLoad,
      Response: dataResponse,
      Message: error.message,
      LastUpdatedBy: functionName,
      LastUpdatedAt: moment.tz('America/Chicago').format(),
    };
    await updateDynamoRow({ housebill, statusCode: originalStatus, data });
    //* Send an SNS to Support Team about the FAILED record.
    return await publishSNSTopic({
      id: housebill,
      status: originalStatus,
      message: `Error Details:${error}`,
      functionName,
    });
  }
}

async function processForConsol({ originalStatus, orderNo, orderId, eventTime }) {
  //* Get shipments for the consol
  const consolidatedShipments = await getAparDataByConsole({ orderNo });
  console.info(
    '🙂 -> file: index.js:330 -> processForConsol -> consolidatedShipments:',
    consolidatedShipments
  );
  const failedRecords = [];
  const successRecords = [];
  for (const shipment of consolidatedShipments) {
    try {
      let finalStatus = originalStatus;
      if (originalStatus.includes(milestones.DEL) || originalStatus.includes(milestones.DWP)) {
        //* Modify the status for DEL and DWP based on the customer and consignee address
        const conIsCu = await consigneeIsCustomer(
          _.get(shipment, 'FK_OrderNo'),
          types.CONSOL,
          _.get(shipment, 'SeqNo')
        );
        if (conIsCu) {
          console.info('Consignee is Customer. Send event DEL or DWP');
        } else {
          //* If the customer and consignee address is not same update the final milestone to AAD (Arrived atb destination)
          const data = {
            Status: status.SKIPPED,
            LastUpdatedBy: functionName,
            LastUpdatedAt: moment.tz('America/Chicago').format(),
          };
          await updateDynamoRow({ housebill: orderNo, statusCode: originalStatus, data });
          finalStatus = milestones.AAD;
        }
      }

      //* Fetch shipment header data using
      const shipmentHeaderData = await getShipmentHeaderData({
        orderNo: _.get(shipment, 'FK_OrderNo'),
      });
      console.info(
        '🙂 -> file: index.js:362 -> processForConsol -> shipmentHeaderData:',
        shipmentHeaderData
      );
      const housebill = _.get(shipmentHeaderData, '[0].Housebill');
      console.info('🙂 -> file: index.js:115 -> housebill:', housebill);
      const XMLpayLoad = await generateNonConsolXmlPayload({
        Housebill: housebill,
        StatusCode: finalStatus,
        EventDateTime: eventTime,
      });
      console.info('🙂 -> file: index.js:359 -> processForConsol -> XMLpayLoad:', XMLpayLoad);
      const dataResponse = await addMilestoneApiDataForNonConsol(XMLpayLoad);
      console.info('🙂 -> file: index.js:361 -> processForConsol -> dataResponse:', dataResponse);
      //* For the success records add the shipment to the success shipment list
      successRecords.push({
        ConsolNo: orderNo,
        FK_OrderNo: _.get(shipment, 'FK_OrderNo'),
        Response: dataResponse,
        Payload: XMLpayLoad,
      });
    } catch (error) {
      console.error('🙂 -> file: index.js:369 -> processForConsol -> error:', error);
      //* For the failed records add the shipment to the failed shipment list
      failedRecords.push({
        ConsolNo: orderNo,
        FK_OrderNo: _.get(shipment, 'FK_OrderNo'),
        ErrorMessage: error.message,
      });
    }
  }

  if (failedRecords.length) {
    //* If there's any failed record mark the update as failed.
    const data = {
      Status: status.FAILED,
      SuccessRecords: successRecords,
      FailedRecords: failedRecords,
      LastUpdatedBy: functionName,
      LastUpdatedAt: moment.tz('America/Chicago').format(),
    };
    return await updateDynamoRow({ housebill: orderNo, statusCode: originalStatus, data });
  }

  //* If milestone is DEL or DWP and there's no failed record, add a record for the order no to check and upload POD
  if (originalStatus.includes(milestones.DEL) || originalStatus.includes(milestones.DWP)) {
    const insertParam = {
      OrderId: orderId,
      EventDateTime: eventTime,
      Housebill: orderNo,
      StatusCode: milestones.POD,
      Status: status.PENDING,
      Type: types.CONSOL,
      RetryCount: 0,
      LastUpdatedBy: functionName,
      LastUpdatedAt: moment.tz('America/Chicago').format(),
    };
    await updateMilestone(insertParam);
  }

  //* If milestone for all the shipments are updated successfully, mark the record as SUCCESS
  const data = {
    Status: status.SENT,
    SuccessRecords: successRecords,
    FailedRecords: failedRecords,
    LastUpdatedBy: functionName,
    LastUpdatedAt: moment.tz('America/Chicago').format(),
  };
  return await updateDynamoRow({ housebill: orderNo, statusCode: originalStatus, data });
}

async function processForMultiStop({ originalStatus, orderNo, orderId, eventTime }) {
  try {
    const consolNo = orderNo;

    //* Get total stop for that multi stop shipment for check for POD only after the final sequence
    const totalStopRes = await getTotalStop({ consolNo });
    const totalStop = totalStopRes?.length;
    console.info('🙂 -> file: index.js:204 -> module.exports.handler= -> totalStop:', totalStop);

    //* Extract the stop sequence to update the shipment for only that sequence
    const stopSeq = originalStatus.split('#')[1];
    console.info('🙂 -> file: index.js:174 -> module.exports.handler= -> stopSeq:', stopSeq);

    //* If there's a # present in the status, that means the update is sequence specific not for all the shipments
    if (originalStatus.includes('#')) {
      //* Get the shipment for the current stop only
      const consolidatedShipments = await getShipmentForStop({
        consolNo,
        stopSeq,
      });
      console.info(
        '🙂 -> file: index.js:93 -> module.exports.handler= -> consolidatedShipments:',
        consolidatedShipments
      );

      const failedRecords = [];
      const successRecords = [];
      for (const shipment of consolidatedShipments) {
        console.info(
          '🙂 -> file: index.js:298 -> processForMultiStop -> shipment:',
          JSON.stringify(shipment)
        );
        let finalStatus = originalStatus;
        let housebill;
        let XMLpayLoad;
        let dataResponse;

        try {
          if (originalStatus.includes(milestones.DEL) || originalStatus.includes(milestones.DWP)) {
            //* Modify the status for DEL and DWP based on the customer and consignee address
            const conIsCu = await consigneeIsCustomer(
              _.get(shipment, 'FK_OrderNo'),
              types.MULTISTOP
            );
            console.info(
              '🙂 -> file: index.js:209 -> module.exports.handler= -> conIsCu:',
              conIsCu
            );
            if (conIsCu) {
              console.info('send event DEL or DWP');
              //* If consignee and customer address is same, update the status to DEL or DWP respectively
              if (originalStatus.includes(milestones.DEL)) finalStatus = milestones.DEL;
              if (originalStatus.includes(milestones.DWP)) finalStatus = milestones.DWP;
            } else {
              await updateStatusTable(consolNo, finalStatus, 'SKIPPED');
              //* If the customer and consignee address is not same update the final milestone to AAD (Arrived atb destination)
              const data = {
                Status: status.SKIPPED,
                LastUpdatedBy: functionName,
                LastUpdatedAt: moment.tz('America/Chicago').format(),
              };
              await updateDynamoRow({
                housebill: consolNo,
                statusCode: finalStatus,
                data,
              });
              finalStatus = milestones.AAD;
              console.info('Send event AAD');
            }
          }

          const shipmentHeaderData = await getShipmentHeaderData({
            orderNo: _.get(shipment, 'FK_OrderNo'),
          });
          housebill = _.get(shipmentHeaderData, '[0].Housebill');
          console.info('🙂 -> file: index.js:344 -> processForMultiStop -> housebill:', housebill);
          XMLpayLoad = await generateNonConsolXmlPayload({
            StatusCode: finalStatus.split('#')[0],
            Housebill: housebill,
            EventDateTime: eventTime,
          });
          console.info(
            '🙂 -> file: index.js:353 -> processForMultiStop -> XMLpayLoad:',
            XMLpayLoad
          );

          dataResponse = await addMilestoneApiDataForNonConsol(XMLpayLoad);
          console.info(
            '🙂 -> file: index.js:351 -> processForMultiStop -> dataResponse:',
            dataResponse
          );

          successRecords.push({
            ConsolNo: orderNo,
            FK_OrderNo: _.get(shipment, 'FK_OrderNo'),
            Housebill: housebill,
            Response: dataResponse,
            Payload: XMLpayLoad,
          });
        } catch (error) {
          failedRecords.push({
            ConsolNo: orderNo,
            FK_OrderNo: _.get(shipment, 'FK_OrderNo'),
            ErrorMessage: error.message,
            Housebill: housebill,
            Response: dataResponse,
            Payload: XMLpayLoad,
          });
        }
      }

      if (failedRecords.length) {
        return await updateStatusTable(
          consolNo,
          originalStatus,
          status.FAILED,
          '',
          '',
          failedRecords
        );
      }
      if (
        (originalStatus.includes(milestones.DEL) || originalStatus.includes(milestones.DWP)) &&
        Number(totalStop) === Number(stopSeq)
      ) {
        const insertParam = {
          OrderId: orderId,
          EventDateTime: eventTime,
          Housebill: orderNo,
          StatusCode: milestones.POD,
          Status: status.PENDING,
          Type: types.MULTISTOP,
          RetryCount: 0,
          LastUpdatedBy: functionName,
          LastUpdatedAt: moment.tz('America/Chicago').format(),
        };
        console.info(
          '🙂 -> file: index.js:305 -> originalStatus.includes -> insertParam:',
          insertParam
        );
        await updateMilestone(insertParam);
      }

      //* If milestone for all the shipments are updated successfully, mark the record as SUCCESS
      const data = {
        Status: status.SENT,
        SuccessRecords: successRecords,
        FailedRecords: failedRecords,
        LastUpdatedBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      };
      return await updateDynamoRow({ housebill: orderNo, statusCode: originalStatus, data });
    }

    let XMLpayLoad;
    let dataResponse;
    try {
      XMLpayLoad = await generateMultiStopXmlPayload({
        consolNo,
        eventDateTime: eventTime,
        statusCode: originalStatus,
      });
      console.info('XML Payload Generated :', XMLpayLoad);

      dataResponse = await addMilestoneApiDataForConsol(XMLpayLoad);
      console.info('dataResponse', dataResponse);

      const data = {
        Status: status.SENT,
        Payload: XMLpayLoad,
        Response: dataResponse,
        LastUpdatedBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      };

      return await updateDynamoRow({ housebill: orderNo, statusCode: originalStatus, data });
    } catch (error) {
      console.error('Error in processForMultiStop', error);
      const data = {
        Status: status.FAILED,
        Payload: XMLpayLoad,
        Response: dataResponse,
        Message: error.message,
        LastUpdatedBy: functionName,
        LastUpdatedAt: moment.tz('America/Chicago').format(),
      };
      return await updateDynamoRow({ housebill: orderNo, statusCode: originalStatus, data });
    }
  } catch (error) {
    console.error('🙂 -> file: index.js:428 -> processForMultiStop -> error:', error);
  }
  return true;
}
