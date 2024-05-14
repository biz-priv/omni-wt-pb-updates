const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone} = require('../shared/dynamo');
const moment = require('moment-timezone');
const sns = new AWS.SNS();

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
                    Id,
                    StatusCode,
                    Housebill: Housebill.toString(),
                    EventDateTime: moment.tz('America/Chicago').format(),
                    Payload: '',
                    Response: '',
                    ErrorMessage: '',
                    Status: 'PENDING'
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
                    Id,
                    StatusCode,
                    Housebill: Housebill.toString(),
                    EventDateTime: moment.tz('America/Chicago').format(),
                    Payload: '',
                    Response: '',
                    ErrorMessage: '',
                    Status: 'PENDING'
                };

                console.info(finalPayload);
                await updateMilestone(finalPayload)

            }
        });

        await Promise.all(promises);
    } catch (error) {
        console.error('Error in handler:', error);
        
        await updateMilestone({
            Id ,
            EventDateTime: moment.tz('America/Chicago').format(),
            Housebill: Housebill.toString(),
            ErrorMessage: error.message,
            StatusCode: 'FAILED'
          });
        
          await publishSNSTopic({
            message: `Error processing Id: ${Id}, ${e.message}. \n Please check the error meesage in DynamoDb Table ${ADD_MILESTONE_TABLE_NAME} for complete error`,
            Id
          });
        throw error
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
  