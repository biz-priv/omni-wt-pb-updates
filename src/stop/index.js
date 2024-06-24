const AWS = require('aws-sdk');
const _ = require('lodash');
const {
  getMovementOrder,
  getOrder,
  updateMilestone,
  getMovement,
  getStop,
} = require('../shared/dynamo');
const moment = require('moment-timezone');

const { ERROR_SNS_TOPIC_ARN, ADD_MILESTONE_TABLE_NAME } = process.env;

exports.handler = async (event) => {
  console.info('Event: ', JSON.stringify(event));

  const records = _.get(event, 'Records', []);
  if (_.get(records, 'eventName') === 'INSERT' || _.get(records, 'eventName') === 'REMOVE') {
    console.info('SKipping Insert/Remove Event');
    return;
  }
  const promises = records.map(async (record) => {
    let stopId;
    let StatusCode;
    let Housebill;
    try {
      const newUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
      const oldUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);

      stopId = _.get(newUnmarshalledRecord, 'id', '');
      console.info('id coming from stop table:', stopId);

      const oldActualArrival = _.get(oldUnmarshalledRecord, 'actual_arrival', '');
      const newActualArrival = _.get(newUnmarshalledRecord, 'actual_arrival', '');
      const newStopType = _.get(newUnmarshalledRecord, 'stop_type', '');

      const oldActualDeparture = _.get(oldUnmarshalledRecord, 'actual_departure', '');
      const newActualDeparture = _.get(newUnmarshalledRecord, 'actual_departure', '');

      const oldConfirmed = _.get(oldUnmarshalledRecord, 'confirmed', '');
      const newConfirmed = _.get(newUnmarshalledRecord, 'confirmed', '');

      console.info('Old Actual Arrival: ', oldActualArrival);
      console.info('New Actual Arrival: ', newActualArrival);

      console.info('Old Actual Departure: ', oldActualDeparture);
      console.info('New Actual Departure: ', newActualDeparture);

      console.info('Old Confirmed: ', oldConfirmed);
      console.info('New Confirmed: ', newConfirmed);

      console.info('New Stop Type: ', newStopType);

      const orderId = _.get(newUnmarshalledRecord, 'order_id', '');
      console.info('Order Id coming from the Event:',orderId)

      const totalSequenceSteps = await getStop(orderId);
      const maxSequenceId = _.size(totalSequenceSteps);
      console.info('Number of Records in Stop table for this record:', maxSequenceId); //max value

      const seqId = await _.get(newUnmarshalledRecord, 'movement_sequence', '');
      console.info('Sequence Id for the Record from Stop table for this record:', seqId); //seqId

      //Status Code = APL
      if (
        (oldActualArrival === '' || oldActualArrival === null) &&
        newActualArrival !== null &&
        newActualArrival !== '' &&
        newStopType === 'PU'
      ) {
        if (Number(seqId) - 1 === 0) {
          console.info('The First Pickup of the Consolidation');
          StatusCode = 'APL';
          console.info('Sending Status Code: ', StatusCode);
          const finalPayload = await getPayloadForStopDb(StatusCode, stopId, newStopType);
          await updateMilestone(finalPayload);
        }
        console.info('This is not the first Pickup of the consolidation');
      }

      //status Code = TTC
      if (
        (oldActualDeparture === null || oldActualDeparture === '') &&
        newActualDeparture !== null &&
        newActualDeparture !== '' &&
        newStopType === 'PU'
      ) {
        StatusCode = 'TTC';
        console.info('Sending Status Code: ', StatusCode);
        const finalPayload = await getPayloadForStopDb(StatusCode, stopId, newStopType);
        await updateMilestone(finalPayload);
        //adding COB/IN Transit
        StatusCode = 'COB';
        console.info('Sending Status Code: ', StatusCode);
        finalPayload = await getPayloadForStopDb(StatusCode, stopId, newStopType);
        await updateMilestone(finalPayload);
      }

      //status Code = AAD
      if (
        (oldActualArrival === '' || oldActualArrival === null) &&
        newActualArrival !== null &&
        newActualArrival !== '' &&
        newStopType === 'SO'
      ) {
        if (maxSequenceId - Number(seqId) === 0) {
          console.info('The Last Delivery of the Consolidation');
          StatusCode = 'AAD';
          console.info('Sending Status Code: ', StatusCode);
          const finalPayload = await getPayloadForStopDb(StatusCode, stopId, newStopType);
          await updateMilestone(finalPayload);
        }
        console.info('This is not the last delivery of the consolidation');
      }

      //status Code = APP
      if (oldConfirmed !== 'Y' && newConfirmed === 'Y' && newStopType === 'PU') {
        StatusCode = 'APP';
        console.info('Sending Status Code: ', StatusCode);
        const finalPayload = await getPayloadForStopDb(StatusCode, stopId, newStopType);
        await updateMilestone(finalPayload);
      }

      //status Code = APD
      if (oldConfirmed !== 'Y' && newConfirmed === 'Y' && newStopType === 'SO') {
        StatusCode = 'APD';
        console.info('Sending Status Code: ', StatusCode);
        const finalPayload = await getPayloadForStopDb(StatusCode, stopId, newStopType);
        await updateMilestone(finalPayload);
      }
    } catch (error) {
      console.error('Error in handler:', error);
      // id, statuscode, housebill, eventdatetime, sattus,
      await updateMilestone({
        Id: stopId,
        EventDateTime: moment.tz('America/Chicago').format(),
        Housebill: Housebill.toString(),
        ErrorMessage: error.message,
        StatusCode: 'FAILED',
      });
      //add sns here
      await publishSNSTopic({
        message: `Error processing StopId: ${stopId}, ${error.message}. \n Please check the error meesage in DynamoDb Table ${ADD_MILESTONE_TABLE_NAME} for complete error`,
        stopId,
      });
      throw error;
    }
  });

  await Promise.all(promises);
};

async function getPayloadForStopDb(StatusCode, stopId, stopType) {
  try {
    const movementId = await getMovement(stopId, stopType);

    const order_id = await getMovementOrder(movementId);
    const Housebill = await getOrder(order_id);

    const finalPayload = {
      OrderId: order_id,
      StatusCode,
      Housebill: Housebill.toString(),
      EventDateTime: moment.tz('America/Chicago').format(),
      Payload: '',
      Response: '',
      ErrorMessage: '',
      Status: 'READY',
    };

    console.info('Payload for add milestone:', finalPayload);
    return finalPayload;
  } catch (error) {
    console.error('Error in getPayloadForStopDb function: ', error);
    throw error;
  }
}

async function publishSNSTopic({ Id, message }) {
  try {
    const params = {
      TopicArn: ERROR_SNS_TOPIC_ARN,
      Subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${STAGE} ~ Id: ${Id}`,
      Message: `An error occurred in ${functionName}: ${message}`,
    };

    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}
