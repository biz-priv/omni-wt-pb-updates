const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone, getMovement} = require('../shared/dynamo');
const moment = require('moment-timezone');

exports.handler = async (event) => {

    let Id;
    let StatusCode;
    let Housebill;

    try {
        const records = _.get(event, 'Records', []);
        const promises = records.map(async (record) => {
            const newUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
            const oldUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);

            stopId = _.get(newUnmarshalledRecord, 'id');
            console.info('id coming from stop table:', Id);

            const oldActualArrival = _.get(oldUnmarshalledRecord, 'actual_arrival');
            const newActualArrival = _.get(newUnmarshalledRecord, 'actual_arrival');
            const newStopType = _.get(newUnmarshalledRecord, 'stop_type');

            const oldActualDeparture = _.get(oldUnmarshalledRecord, 'actual_departure');
            const newActualDeparture = _.get(newUnmarshalledRecord, 'actual_departure');

            const oldConfirmed = _.get(oldUnmarshalledRecord, 'confirmed');
            const newConfirmed = _.get(newUnmarshalledRecord, 'confirmed');


            //Status Code = APL
            if(oldActualArrival==null & newActualArrival!=null & newStopType==='PU'){
                StatusCode = 'APL';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }

            //status Code = TTC
            if(oldActualDeparture==null & newActualDeparture!=null & newStopType==='PU'){
                StatusCode = 'TTC';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }

            //status Code = AAD
            if(oldActualArrival==null & newActualArrival!=null & newStopType==='SO'){
                StatusCode = 'AAD';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }
            
            //status Code = DWP
            if(oldActualDeparture==null & newActualDeparture!=null & newStopType==='SO'){
                StatusCode = 'DWP';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }

            //status Code = APP
            if(oldConfirmed!='Y' & newConfirmed==='Y' & newStopType==='PU'){
                StatusCode = 'APP';
                const finalPayload = await getPayloadForStopDb(StatusCode, stopId);
                await updateMilestone(finalPayload)
            }

            //status Code = APD
            if(oldConfirmed!='Y' & newConfirmed==='Y' & newStopType==='SO'){
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
            Id ,
            EventDateTime: moment.tz('America/Chicago').format(),
            Housebill: Housebill.toString(),
            ErrorMessage: error.message,
            StatusCode: 'FAILED'
          });
        throw error
    }
};

async function getPayloadForStopDb(StatusCode, stopId){
    try{
        const movementId = await getMovement(stopId);
        const order_id  = await getMovementOrder(movementId);
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

        console.info("Payload for add milestone:", finalPayload);
        return finalPayload;
    }
    catch(error){
        console.error("Error in getPayloadForStopDb function: ", error);
        throw error
    }
}