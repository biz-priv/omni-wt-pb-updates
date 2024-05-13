const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone} = require('../shared/dynamo');
const moment = require('moment-timezone');
const axios = require('axios');
const sns = new AWS.SNS();
const { get } = require("lodash");

const { ERROR_SNS_TOPIC_ARN, ADD_MILESTONE_TABLE_NAME} = process.env;

let functionName;

let itemObj = {
    Id:"",
    Housebill: "",
    StatusCode: "",
    EventDateTime: "",
    Payload: "",
    Reponse: "",
    ErrorMessage: "",
    Status: ""
}

module.exports.handler = async (event, context) => {
    console.info("Test lambda has been triggered on Dynamo Trigger With Filter Expression.");

    try {
        const records = _.get(event, 'Records', []);
        for (const record of records) {
            const newUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

            itemObj.Id = _.get(newUnmarshalledRecord, "Id");
            itemObj.Housebill = _.get(newUnmarshalledRecord, "Housebill");
            itemObj.StatusCode = _.get(newUnmarshalledRecord, "StatusCode");
            itemObj.EventDateTime = moment.tz('America/Chicago').format(); 
            itemObj.Payload = JSON.stringify({
                Id: itemObj.Id,
                Housebill: itemObj.Housebill,
                StatusCode: itemObj.StatusCode,
                EventDateTime: itemObj.EventDateTime
            });

            console.info('Processed Item:', itemObj);
        }

    } catch (error) {
        console.error("Error processing event:", error);
        await publishSNSTopic({ Id: itemObj.Id, message: error.message });
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
  
async function addMilestoneApi(postData) {
    try {

        const config = {
            method: 'post',
            headers: {
                'Accept': 'text/xml',
                'Content-Type': 'text/xml'
            },
            data: postData
        };

        if (get(itemObj, "statusCode", "") === "DEL") {
            config.url = `${process.env.ADD_MILESTONE_URL}?op=SubmitPOD`;
        } else if (get(itemObj, "statusCode", "") === "LOC" || get(itemObj, "statusCode", "") === "OTH") {
            config.url = `${process.env.ADD_MILESTONE_LOC_URL}?op=WriteTrackingNote`;
        } else {
            config.url = `${process.env.ADD_MILESTONE_URL}?op=UpdateStatus`;
        }

        console.log("config: ", config)
        const res = await axios.request(config);
        if (get(res, "status", "") == 200) {
            return get(res, "data", "");
        } else {
            itemObj.xmlResponsePayload = get(res, "data", "");
            throw new Error(`API Request Failed: ${res}`);
        }
    } catch (error) {
        console.error("e:addMilestoneApi", error);
        throw error;
    }
} 