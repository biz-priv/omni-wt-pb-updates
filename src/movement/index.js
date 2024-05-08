const AWS = require('aws-sdk');
const _ = require('lodash');
const { getMovementOrder,getOrder,updateMilestone} = require('./shared/dynamo');

exports.handler = async (event) => {
    try {
        const records = _.get(event, 'Records', []);
        const promises = records.map(async (record) => {
            const newUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
            const oldUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);

            const id = _.get(newUnmarshalledRecord, 'id');
            console.info('id coming from movement table:', id);

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
                const wtStatusCodeValue = 'DIS';

                console.info("Value of Id", id);

                const order_id  = await getMovementOrder(id);
                const housebill_num  = await getOrder(order_id);

                const finalPayload = {
                    id: id,
                    WTStatusCodes: wtStatusCodeValue,
                    housebill: housebill_num.toString(),
                    systemdate: '',
                    recordstatus: ''
                };

                console.info(finalPayload);
                await updateMilestone(finalPayload)
            }

            if (oldStatus === 'A' && newStatus === 'C') {
                const wtStatusCodeValue = 'BOO';

                console.info("Value of Id", id);

                const order_id  = await getMovementOrder(id);
                const housebill_num  = await getOrder(order_id);

                const finalPayload = {
                    id: id,
                    WTStatusCodes: wtStatusCodeValue,
                    housebill: housebill_num.toString(),
                    systemdate: '',
                    recordstatus: ''
                };

                console.info(finalPayload);
                await updateMilestone(finalPayload)

            }
        });

        await Promise.all(promises);
    } catch (error) {
        console.error('Error in handler:', error);
        throw error
    }
};