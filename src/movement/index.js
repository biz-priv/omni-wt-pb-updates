const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone} = require('../shared/dynamo');
const moment = require('moment-timezone');
const sns = new AWS.SNS();
const axios = require('axios');

const { ERROR_SNS_TOPIC_ARN, ADD_MILESTONE_TABLE_NAME} = process.env;

let functionName;

module.exports.handler = async (event,context) => {

    let Id;
    let StatusCode;
    let Housebill;

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
                StatusCode = 'DIS';

                console.info("Value of Id", Id);

                const order_id  = await getMovementOrder(Id);
                Housebill  = await getOrder(order_id);

                const finalPayload = {
                    OrderId: order_id,
                    StatusCode,
                    Housebill: Housebill.toString(),
                    EventDateTime: moment.tz('America/Chicago').format(),
                    Payload: '',
                    Response: '',
                    ErrorMessage: '',
                    Status: 'READY'
                };

                console.info(finalPayload);
                await updateMilestone(finalPayload)
            }

            if (oldStatus === 'A' && newStatus === 'C') {
                StatusCode = 'BOO';

                console.info("Value of Id", Id);

                const order_id  = await getMovementOrder(Id);
                const Housebill  = await getOrder(order_id);

                const finalPayload = {
                    OrderId: order_id,
                    StatusCode,
                    Housebill: Housebill.toString(),
                    EventDateTime: moment.tz('America/Chicago').format(),
                    Payload: '',
                    Response: '',
                    ErrorMessage: '',
                    Status: 'READY'
                };

                console.info(finalPayload);
                await updateMilestone(finalPayload)

            }

            if (oldStatus !== 'D' && newStatus === 'D') {

              const movementId = Id;

              if (!movementId) {
                return {
                    statusCode: 400,
                    body: JSON.stringify('Movement ID is required')
                };
              }

            const podStatus = await checkForPod(movementId);

            let resultMessage;

            if (podStatus === 'Y') {
              resultMessage = "POD is Available";
              StatusCode = 'DEL';
          } else {
              resultMessage = "POD is Unavailable";
              StatusCode = 'DWP';
          }

          console.info('WT status code :',StatusCode)

          const order_id  = await getMovementOrder(Id);
          const Housebill  = await getOrder(order_id);

              const finalPayload = {
                  OrderId: order_id,
                  StatusCode,
                  Housebill: Housebill.toString(),
                  EventDateTime: moment.tz('America/Chicago').format(),
                  Payload: '',
                  Response: '',
                  ErrorMessage: '',
                  Status: 'READY'
              };

              console.info(finalPayload);
              await updateMilestone(finalPayload)

            }

        });

        await Promise.all(promises);
    } catch (error) {
        console.error('Error in handler:', error);
        
        await updateMilestone({
            EventDateTime: moment.tz('America/Chicago').format(),
            Housebill: Housebill.toString(),
            ErrorMessage: error.message,
            StatusCode: 'FAILED'
          });
        
          await publishSNSTopic({
            message: `Error processing Housebill: ${Housebill}, ${e.message}. \n Please check the error meesage in DynamoDb Table ${ADD_MILESTONE_TABLE_NAME} for complete error`,
            Id
          });
        throw error
    }
};

async function publishSNSTopic({ Housebill, message}) {
    try {
      const params = {
        TopicArn: ERROR_SNS_TOPIC_ARN,
        Subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${STAGE} ~ Housebill: ${Housebill}`,
        Message: `An error occurred in ${functionName}: ${message}`
      };
  
      await sns.publish(params).promise();
    } catch (error) {
      console.error('Error publishing to SNS topic:', error);
      throw error;
    }
  }

async function checkForPod(movementId) {
      const username = "apiuser";
      const password = "lvlpapiuser";
      const mcleodHeaders = {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
      };
      const rowType = 'O';
  
      // Basic Auth header
      const authHeader = 'Basic ' + Buffer.from(username + ':' + password).toString('base64');
  
      try {
          // Step 1: Get Order ID from Movement ID
          let url = `https://tms-lvlp.loadtracking.com/ws/api/movements/search?id=${movementId}`;
          let response = await axios.get(url, { headers: { ...mcleodHeaders, 'Authorization': authHeader } });
  
          if (response.status !== 200) {
              let errorMessage = `Failed to get order ID for movement ${movementId}. Status code: ${response.status}`;
              await publishSnsTopic(movementId, errorMessage);
              return 'N';
          }
  
          let output = response.data;
          if (!output || !output[0].orders || !output[0].orders.length) {
              let errorMessage = `No orders found for movement ${movementId}.`;
              await publishSnsTopic(movementId, errorMessage);
              return 'N';
          }
  
          let orderId = output[0].orders[0].id;
  
          // Step 2: Get POD
          url = `https://tms-lvlp.loadtracking.com/ws/api/images/${rowType}/${orderId}`;
          response = await axios.get(url, { headers: { ...mcleodHeaders, 'Authorization': authHeader } });
  
          if (response.status !== 200) {
              let errorMessage = `Failed to get POD for order ${orderId}. Status code: ${response.status}`;
              await publishSnsTopic(movementId, errorMessage);
              return 'N';
          }
  
          output = response.data;
  
          let photoType, photoId, exists;
  
          try {
              if (!output) throw new Error("Empty response");
  
              photoType = output[0].descr.toUpperCase();
  
              // Step 2a: Check to see if there is a POD
              if (photoType === '01-BILL OF LADING') {
                  photoId = output[0].id;
                  exists = 'Y';
              } else {
                  photoId = 'NO POD';
                  exists = 'N';
              }
          } catch (e) {
              let errorMessage = `Error processing POD data: ${e.message}`;
              await publishSnsTopic(movementId, errorMessage);
              photoId = 'NO POD';
              exists = 'N';
          }
  
          console.log(`Does a POD for movement ${movementId} exist? ${exists}`);
          return exists;
  
      } catch (error) {
          let errorMessage = `Error: ${error.message}`;
          await publishSnsTopic(movementId, errorMessage);
          return 'N';
      }
  }