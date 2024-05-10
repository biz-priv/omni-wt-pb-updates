const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const _ = require('lodash');

async function query(params) {
    try {
        return await dynamoDB.query(params).promise();
    } catch (error) {
        console.error('Query Item Error:', error, '\nQuery Params:', params);
        throw error
    }
}

async function getMovementOrder(id) {
    const movementParams = {
        TableName: 'omni-pb-rt-movement-order',
        IndexName: 'movement_id-index',
        KeyConditionExpression: 'movement_id = :movement_id',
        ExpressionAttributeValues: {
            ':movement_id': id
        }
    };
    
    try {
        const result = await query(movementParams);
        const items = _.get(result, 'Items', []);

        if (items.length > 0) {
            const orderId = _.get(items, '[0]order_id');
            console.info('Order ID:', orderId);
            return orderId;
        } else {
            throw new Error('No Record found in Movement order Dynamo Table for Id:',id)
        }
    } catch (error) {
        console.error('Error in getMovementOrder function:', error);
        throw error
    }
}

async function getOrder(orderId) {
    const orderParams = {
        TableName: 'omni-pb-rt-orders-dev',
        KeyConditionExpression: 'id = :id',
        ExpressionAttributeValues: {
            ':id': orderId
        }
    };

    console.log('Fetching data from:', orderParams.TableName);

    try {
        const result = await query(orderParams);
        const items = _.get(result, 'Items', []);

        if (items.length > 0) {
            const housebillNum = _.get(items, '[0]blnum');
            console.info('Housebill Number:', housebillNum);
            return housebillNum;
        } else {
            throw new Error('No Record found in Orders Dynamo Table for orderId:',orderId)
        }
    } catch (error) {
        console.error('Error in getOrder function:', error);
        throw error
    }
}

async function updateMilestone(finalPayload){
    const addMilestoneParams = {
        TableName: 'omni-pb-214-add-milestone',
        Item: finalPayload
    };
    try {
        await dynamoDB.put(addMilestoneParams).promise();
        console.log('Successfully inserted payload into omni-pb-214-add-milestone table');
    } catch (error) {
        console.error('Error while udating data into 214-add-milestone Dynamo Table',error)
        throw error
    }
}

async function getMovement(id){
    const movementParams = {
        TableName: process.env['MOVEMENT_DB'],
        IndexName: 'OrderStopIndex',
        KeyConditionExpression: 'origin_stop_id = :id',
        FilterExpression: 'status IN (:statusA, :statusC)',
        ExpressionAttributeValues: {
            ':id': id,
            ':statusA': 'a',
            ':statusC': 'c'
        }    
    }

    try {
        const result = await query(movementParams);
        console.log(result);
        const items = _.get(result, 'Items', []);
        return get(result, 'id', '');

    } catch (error) {
        console.error('Error in getMovement function:', error);
        throw error
    }
}

module.exports = {
    getMovementOrder,
    getOrder,
    updateMilestone,
    getMovement
};