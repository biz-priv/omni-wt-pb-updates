// const AWS = require('aws-sdk');
// const axios = require('axios');
// const moment = require('moment-timezone'); 
// const { get } = require('lodash');
// const dynamo = new AWS.DynamoDB.DocumentClient();
// const ADD_MILESTONE_LOGS_TABLE = 'ADD_MILESTONE_LOGS_TABLE';

// const sendEvent = async (finalPayload) => {
//     const apiUrl = 'test';  ///need to get the API Url
//     try {
//         const response = await axios.post(apiUrl, finalPayload);
//         return { statusCode: 200, body: JSON.stringify(response.data) };
//     } catch (error) {
//         console.error('Error sending event to API:', error);
//         throw new Error(`API request failed: ${error.message}`);
//     }
// };

// const putItem = async (tableName, item) => {
//     const params = {
//         TableName: tableName,
//         Item: item,
//     };
//     return dynamo.put(params).promise();
// };

// module.exports.handler = async (event, context) => {
//     console.log("Event: ", JSON.stringify(event));
//     let itemObj = {};
//     try {
//         const body = event; // Assuming the whole event is the body or adapt as needed

//         // Constructing final payload
//         const finalPayload = {
//             Id: get(body, "addMilestoneRequest.Id", ""),
//             StatusCode: get(body, "addMilestoneRequest.statusCode", ""),
//             Housebill: get(body, "addMilestoneRequest.housebill", "").toString(),
//             EventDateTime: moment.tz('America/Chicago').format(),
//             Payload: JSON.stringify(body), // You might want to adjust what exactly goes into Payload
//             Response: '', 
//             ErrorMessage: '',
//             Status: 'PENDING'
//         };

//         // Additional validations or operations can be performed here
//         // For example, using validation and conditional logic similar to previous examples

//         // Sending final payload to an external API
//         const apiResponse = await sendEvent(finalPayload);
//         console.log('API response:', apiResponse);

//         // Optionally updating the database with the response or status
//         finalPayload.Response = apiResponse.body; // Storing the API response
//         await putItem(ADD_MILESTONE_LOGS_TABLE, finalPayload);

//         return apiResponse;

//     } catch (error) {
//         console.error("Main lambda error:", error);
//         itemObj = {
//             ...itemObj,
//             ErrorMessage: error.message || String(error),
//             Status: 'ERROR'
//         };
//         await putItem(ADD_MILESTONE_LOGS_TABLE, itemObj);
//         return { statusCode: 400, message: error.message || "An error occurred" };
//     }
// };


const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone} = require('../shared/dynamo');
const moment = require('moment-timezone');
const sns = new AWS.SNS();

const { ERROR_SNS_TOPIC_ARN, ADD_MILESTONE_TABLE_NAME} = process.env;

let functionName;

module.exports.handler = async (event,context) => {
  
    console.info("Test lambda has been triggered on Dynamo Trigger WITH fILTER EXPRESSION.")

    let sqsEventRecords = [];

    try {
        console.log("event", JSON.stringify(event));
        sqsEventRecords = event.Records;

        sqsEventRecords.forEach(record => {
            console.log("Processing record:", JSON.stringify(record));
        });

    } catch (error) {
        console.error("Error processing event:", error);        
    }

};

async function publishSNSTopic({ Id, message}) {
    try {
      const params = {
        TopicArn: ERROR_SNS_TOPIC_ARN,
        Subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${STAGE} ~ Id: ${Id}`,
        Message: `An error occurred in ${functionName}: ${message}`
      };
  
      await sns.publish(params).promise();
    } catch (error) {
      console.error('Error publishing to SNS topic:', error);
      throw error;
    }
  }
  