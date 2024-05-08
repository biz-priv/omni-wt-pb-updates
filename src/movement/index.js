const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone} = require('../shared/dynamo');
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