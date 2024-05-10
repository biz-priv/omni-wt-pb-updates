const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const _ = require('lodash');

const { ORDERS_TABLE_NAME, MOVEMENT_ORDER_TABLE_NAME, ADD_MILESTONE_TABLE_NAME } = process.env;

async function query(params) {
    async function helper(params) {
        try {
            const result = await dynamoDB.query(params).promise();
            let data = _.get(result, 'Items', []);

            if (result.LastEvaluatedKey) {
                params.ExclusiveStartKey = result.LastEvaluatedKey;
                data = data.concat(await helper(params));
            }

            return data;
        } catch (error) {
            console.error('Query Item Error:', error, '\nQuery Params:', params);
            throw error;
        }
    }

    return helper(params);
}

async function getMovementOrder(id) {
    const movementParams = {
        TableName: MOVEMENT_ORDER_TABLE_NAME,
        IndexName: 'movement_id-index',
        KeyConditionExpression: 'movement_id = :movement_id',
        ExpressionAttributeValues: {
            ':movement_id': id
        }
    };

    try {
        const items = await query(movementParams);

        if (items.length > 0) {
            const orderId = _.get(items, '[0]order_id');
            console.info('Order ID:', orderId);
            return orderId;
        } else {
            throw new Error('No Record found in Movement order Dynamo Table for Id:', id);
        }
    } catch (error) {
        console.error('Error in getMovementOrder function:', error);
        throw error;
    }
}

async function getOrder(orderId) {
    const orderParams = {
        TableName: ORDERS_TABLE_NAME,
        KeyConditionExpression: 'id = :id',
        ExpressionAttributeValues: {
            ':id': orderId
        }
    };

    console.log('Fetching data from:', orderParams.TableName);

    try {
        const items = await query(orderParams);

        if (items.length > 0) {
            const housebillNum = _.get(items, '[0]blnum');
            console.info('Housebill Number:', housebillNum);
            return housebillNum;
        } else {
            throw new Error('No Record found in Orders Dynamo Table for orderId:', orderId);
        }
    } catch (error) {
        console.error('Error in getOrder function:', error);
        throw error;
    }
}

async function updateMilestone(finalPayload) {
    const addMilestoneParams = {
        TableName: ADD_MILESTONE_TABLE_NAME,
        Item: finalPayload
    };
    try {
        await dynamoDB.put(addMilestoneParams).promise();
        console.log('Successfully inserted payload into omni-pb-214-add-milestone table');
    } catch (error) {
        console.error('Error while updating data into 214-add-milestone Dynamo Table', error);
        throw error;
    }
}

module.exports = {
    getMovementOrder,
    getOrder,
    updateMilestone
};