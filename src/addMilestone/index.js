const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone} = require('../shared/dynamo');
const moment = require('moment-timezone');
const axios = require('axios');
const sns = new AWS.SNS();
const { get } = require("lodash");
const { js2xml } = require('xml-js');
const dynamoDb = new AWS.DynamoDB.DocumentClient();

const { ENVIRONMENT,ERROR_SNS_TOPIC_ARN, ADD_MILESTONE_TABLE_NAME, WT_SOAP_USERNAME, ADD_MILESTONE_URL} = process.env;

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

            const XMLpayLoad = await makeJsonToXml(itemObj)
            console.info("XML Payload Generated :",XMLpayLoad)

            const dataResponse = await addMilestoneApi(XMLpayLoad);
            console.info("dataResponse", dataResponse);
            itemObj.Reponse = dataResponse;

            await updateStatusTable(itemObj.Housebill, itemObj.StatusCode,XMLpayLoad, dataResponse)
        }

    } catch (error) {
        console.error("Error processing event:", error);
        // await publishSNSTopic({ Id: itemObj.Id, message: error.message });
        throw error;
    }
};

async function publishSNSTopic({ Id, message}) {
    try {
      const params = {
        TopicArn: ERROR_SNS_TOPIC_ARN,
        Subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ Id: ${Id}`,
        Message: `An error occurred in ${functionName}: ${message}`
      };
  
      await sns.publish(params).promise();
    } catch (error) {
      console.error('Error publishing to SNS topic:', error);
      throw error;
    }
  }

async function makeJsonToXml(itemObj) {
    try {
        const xml = js2xml({
            "soap:Envelope": {
                "_attributes": {
                    "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                    "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                    "xmlns:soap": "http://schemas.xmlsoap.org/soap/envelope/"
                },
                "soap:Body": {
                    UpdateStatus: {
                        "_attributes": {
                            "xmlns": "http://tempuri.org/"
                        },
                        HandlingStation: "",
                        HAWB: _.get(itemObj, "Housebill", ""),
                        UserName: WT_SOAP_USERNAME,
                        StatusCode: _.get(itemObj, "StatusCode", ""),
                        EventDateTime: _.get(itemObj, "EventDateTime", ""),
                    }
                }
            }
        }, {compact: true, ignoreComment: true, spaces: 4});
        console.info("XML payload", xml);
        return xml;
    } catch (error) {
        console.error("Error generating XML:", error);
        return null;
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

        config.url = `${process.env.ADD_MILESTONE_URL}?op=UpdateStatus`;     

        console.log("config: ", config)
        const res = await axios.request(config);
        if (get(res, "status", "") == 200) {
            return get(res, "data", "");
        } else {
            itemObj.xmlResponsePayload = get(res, "data", "");
            throw new Error(`API Request Failed: ${res}`);
        }
    } catch (error) {
        const response = error.response;
        console.error("Error in addMilestoneApi", {
            message: error.message,
            response: {
                status: response?.status,
                data: response?.data
            }
        });
        throw error; 
    }
}

async function updateStatusTable(Housebill,StatusCode, Payload, Response) {
    try {
      const updateParam = {
        TableName: 'omni-pb-214-add-milestone-dev',
        Key: {
            Housebill, 
            StatusCode
        },
        UpdateExpression:
          'set Payload = :payload, #Response = :response, #Status = :status, EventDateTime = :eventDateTime',
        ExpressionAttributeNames: {
          '#Status': 'Status',
          '#Response': 'Response',
        },
        ExpressionAttributeValues: {
            ':payload': String(Payload),
            ':response': String(Response),
              ':status': "PROCESSED",
            ':eventDateTime': moment.tz('America/Chicago').format(),
          },
      };
      console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
      return await dynamoDb.update(updateParam).promise();
    } catch (err) {
      console.error('🙂 -> file: index.js:224 -> err:', err);
      throw err;
    }
  }