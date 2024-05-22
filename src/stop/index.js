const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone, getMovement} = require('../shared/dynamo');
const moment = require('moment-timezone');


exports.handler = async (event) => {

    let stopId;
    let StatusCode;
    let Housebill;

    console.info("Event: ", JSON.stringify(event));
    

    try {
        const records = _.get(event, 'Records', []);
        if(_.get(records, "eventName")==='INSERT' || _.get(records, "eventName")==='REMOVE'){
            console.info("SKipping Insert/Remove Event");
            return;
        }
        const promises = records.map(async (record) => {
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
            
            console.info("Old Actual Arrival: ", oldActualArrival);
            console.info("New Actual Arrival: ", newActualArrival);

            console.info("Old Actual Departure: ", oldActualDeparture);
            console.info("New Actual Departure: ", newActualDeparture);

            console.info("New Stop Type: ", newStopType);

            //Status Code = APL
            if((oldActualArrival==='' || oldActualArrival===null) && (newActualArrival!==null || newActualArrival!=='') && newStopType==='PU'){
                StatusCode = 'APL';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }

            //status Code = TTC
            if((oldActualDeparture===null || oldActualDeparture==='') && (newActualDeparture!==null  || newActualDeparture!=='') && newStopType==='PU'){
                StatusCode = 'TTC';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }

            //status Code = AAD
            if((oldActualArrival==='' || oldActualArrival===null ) && (newActualArrival!==null || newActualArrival!=='') && newStopType==='SO'){
                StatusCode = 'AAD';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }
            
            //status Code = DWP
            // if((oldActualDeparture==='' || oldActualDeparture===null) && (newActualDeparture!==null || newActualDeparture!=='') && newStopType==='SO'){
            //     StatusCode = 'DWP';
            //     const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
            //     await updateMilestone(finalPayload)
            // }

            //status Code = APP
            if(oldConfirmed!='Y' && newConfirmed==='Y' && newStopType==='PU'){
                StatusCode = 'APP';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }

            //status Code = APD
            if(oldConfirmed!='Y' && newConfirmed==='Y' && newStopType==='SO'){
                StatusCode = 'APD';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }
            
        });

        await Promise.all(promises);
    } catch (error) {
        console.error('Error in handler:', error);
        // id, statuscode, housebill, eventdatetime, sattus,
        await updateMilestone({
            Id: stopId ,
            EventDateTime: moment.tz('America/Chicago').format(),
            Housebill: Housebill.toString(),
            ErrorMessage: error.message,
            StatusCode: 'FAILED'
          });
          //add sns here
          await publishSNSTopic({
            message: `Error processing StopId: ${stopId}, ${error.message}. \n Please check the error meesage in DynamoDb Table ${ADD_MILESTONE_TABLE_NAME} for complete error`,
            stopId
          });
        throw error
    }
};

async function getPayloadForStopDb(StatusCode, stopId){
    try{
        const movementId = await getMovement(stopId);
        const order_id  = await getMovementOrder(movementId);
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

        console.info("Payload for add milestone:", finalPayload);
        return finalPayload;
    }
    catch(error){
        console.error("Error in getPayloadForStopDb function: ", error);
        throw error
    }
}

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