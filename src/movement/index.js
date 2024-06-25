const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder, getOrder, updateMilestone } = require('../shared/dynamo');
const moment = require('moment-timezone');
const sns = new AWS.SNS();
const axios = require('axios');

const { ERROR_SNS_TOPIC_ARN, ADD_MILESTONE_TABLE_NAME, ENVIRONMENT } = process.env;

let functionName;

module.exports.handler = async (event, context) => {
  let Id;
  let StatusCode;
  let Housebill;

  console.info('Event coming in ',event)

  try {
    functionName = _.get(context, 'functionName');
    const records = _.get(event, 'Records', []);
    const promises = records.map(async (record) => {
      const newUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
      const oldUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);

      Id = _.get(newUnmarshalledRecord, 'id');
      console.info('id coming from movement table:', Id);

      const oldBrokerageStatus = _.get(oldUnmarshalledRecord, 'brokerage_status');
      const newBrokerageStatus = _.get(newUnmarshalledRecord, 'brokerage_status');
      console.info('oldBrokerageStatus:', oldBrokerageStatus);
      console.info('newBrokerageStatus:', newBrokerageStatus);

      let oldStatus = _.get(oldUnmarshalledRecord, 'status');
      let newStatus = _.get(newUnmarshalledRecord, 'status');
      console.info('oldStatus:', oldStatus);
      console.info('newStatus:', newStatus);

      console.info('brokerage_status coming from movement table is:', newBrokerageStatus);

      if (oldBrokerageStatus !== 'DISPATCH' && newBrokerageStatus === 'DISPATCH') {
        // StatusCode = 'DIS'; //TLD
        StatusCode = 'TLD'; 

        console.info('Value of Id', Id);

        const order_id = await getMovementOrder(Id);
        Housebill = await getOrder(order_id);

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

        console.info(finalPayload);
        await updateMilestone(finalPayload);
      }

      if (oldStatus === 'A' && newStatus === 'C') {
        StatusCode = 'BOO';

        console.info('Value of Id', Id);

        const order_id = await getMovementOrder(Id);
        Housebill = await getOrder(order_id);

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

        console.info(finalPayload);
        await updateMilestone(finalPayload);
      }

      if (oldStatus !== 'D' && newStatus === 'D') {
        const movementId = Id;

        const order_id = await getMovementOrder(Id);
        Housebill = await getOrder(order_id);

        if (!movementId) {
          return {
            statusCode: 400,
            body: JSON.stringify('Movement ID is required'),
          };
        }

        const podStatus = await checkForPod(movementId, order_id);

        let resultMessage;

        if (podStatus === 'Y') {
          resultMessage = 'POD is Available';
          StatusCode = 'DEL';
        } else {
          resultMessage = 'POD is Unavailable';
          StatusCode = 'DWP';
        }

        console.info('WT status code :', StatusCode);

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

        console.info(finalPayload);
        await updateMilestone(finalPayload);
      }
    });

    await Promise.all(promises);
  } catch (error) {
    console.error('Error in handler:', error);

    await updateMilestone({
      EventDateTime: moment.tz('America/Chicago').format(),
      Housebill: Housebill.toString(),
      ErrorMessage: error.message,
      StatusCode: 'FAILED',
    });

    await publishSNSTopic({
      subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ id: ${Id}`,
      message: `Error processing Housebill: ${Housebill}, ${error.message}. \n Please check the error meesage in DynamoDb Table ${ADD_MILESTONE_TABLE_NAME} for complete error`,
    });
    throw error;
  }
};

async function publishSNSTopic({ subject, message }) {
  try {
    const params = {
      TopicArn: ERROR_SNS_TOPIC_ARN,
      Subject: subject,
      Message: message,
    };

    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}

async function checkForPod(movementId, orderId) {
  try {
    const username = 'apiuser';
    const password = 'lvlpapiuser';
    const mcleodHeaders = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    };
    const rowType = 'O';

    console.log('movementID is ',movementId)
    console.log('orderId is ',orderId)
    console.log('Entered POD STATUS CHECK function')

    // Basic Auth header
    const authHeader = 'Basic ' + Buffer.from(username + ':' + password).toString('base64');

    // Get POD
    const url = `https://tms-lvlp.loadtracking.com:6790/ws/api/images/${rowType}/${orderId}`;

    console.info('URL trying to fetch the details:',url)

    const response = await axios.get(url, {
      headers: { ...mcleodHeaders, Authorization: authHeader },
    });

    console.info('RESPONSE: ', response)
    console.info('RESPONSE Status: ', response.status)

    if (response.status !== 200) {
      let errorMessage = `Failed to get POD for order ${orderId}. Status code: ${response.status}`;
      await publishSNSTopic({
        subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ id: ${movementId}`,
        message: errorMessage,
      });
      return 'N';
    }

    console.log('1st If Condition passed')

    const output = response.data;

    console.log('Output tagged:',output)

    let photoType;
    let exists;

    if (!output) throw new Error('Empty response');

    photoType = output[0].descr.toUpperCase();

    //Check to see if there is a POD
    if (photoType === '01-BILL OF LADING') {
      exists = 'Y';
    } else {
      exists = 'N';
    }
    console.info('value in exists :', exists);
    console.log(`Does a POD for movement ${movementId} exist? ${exists}`);
    return exists;
  } catch (error) {
    let errorMessage = `Error processing POD data: ${error.message}`;
    await publishSNSTopic({
      subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ id: ${movementId}`,
      message: errorMessage,
    });
  }
}
