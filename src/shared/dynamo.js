const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const _ = require('lodash');

const { ORDERS_TABLE_NAME, MOVEMENT_ORDER_TABLE_NAME, ADD_MILESTONE_TABLE_NAME, ORDER_STATUS_TABLE_NAME,CONSOL_STATUS_TABLE_NAME } = process.env;

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

async function getMovement(id,stopType){
    const movementParams = {
        TableName: process.env['MOVEMENT_DB'],
        IndexName: stopType==='PU'?'OriginStopIndex':'DestStopIndex',
        KeyConditionExpression: stopType === 'PU' ? 'origin_stop_id = :id' : 'dest_stop_id = :id',
        FilterExpression: '#status IN (:statusP, :statusC)',
        ExpressionAttributeNames: {
            '#status': 'status'
        },
        ExpressionAttributeValues: {    
            ':id': id,
            ':statusP': 'P',
            ':statusC': 'C'
        }    
    }

    try {
        const result = await query(movementParams);
        
        if (result.length > 0) {
            return _.get(result[0], 'id', '');
        } else {
            throw new Error('No Record found in Movement Table for id: ', id);
        }

    } catch (error) {
        console.error('Error in getMovement function:', error);
        throw error
    }
}

async function getLive204OrderStatus(Housebill) {
    const params = {
        TableName: 'live-204-order-status-dev',
        IndexName: 'Housebill-index',
        KeyConditionExpression: 'Housebill = :Housebill',
        ExpressionAttributeValues: {
            ':Housebill': Housebill
        }
    };

    console.log('Fetching data from:', params.TableName);

    try {
        const items = await query(params);

        if (items.length > 0) {
            return items[0];
        } else {
            throw new Error('No Record found in 204 Orders Dynamo Table for Housebill:', Housebill);
        }
    } catch (error) {
        console.error('Error in getLive204OrderStatus function:', error);
        throw error;
    }
}

async function getOrderStatus(id) {
    const orderStatusParams = {
        TableName: ORDER_STATUS_TABLE_NAME,
        IndexName: 'ShipmentId-index',
        KeyConditionExpression: 'ShipmentId = :ShipmentId',
        ExpressionAttributeValues: {
            ':ShipmentId': id
        }
    };

    try {
        const items = await query(orderStatusParams);

        if (items.length > 0) {
            return items[0];
        } else {
            console.info('No Record found in Order Status Dynamo Table for MovementId:', id);
        }
    } catch (error) {
        console.error('Error in getMovementOrder function:', error);
        throw error;
    }
}

async function getConsolStatus(id) {
    const consolStatusParams = {
        TableName: CONSOL_STATUS_TABLE_NAME,
        IndexName: 'ShipmentId-index',
        KeyConditionExpression: 'ShipmentId = :ShipmentId',
        ExpressionAttributeValues: {
            ':ShipmentId': id
        }
    };

    try {
        const items = await query(consolStatusParams);

        if (items.length > 0) {
            return items[0];
        } else {
            console.info('No Record found in Consol Status Dynamo Table for MovementId:', id);
        }
    } catch (error) {
        console.error('Error in getMovementOrder function:', error);
        throw error;
    }
}

async function getStop(order_id) {
    const consolStatusParams = {
        TableName: 'omni-pb-rt-stop-dev',
        IndexName: 'order_id-index',
        KeyConditionExpression: 'order_id = :order_id',
        ExpressionAttributeValues: {
            ':order_id': order_id
        }
    };

    try {
        const items = await query(consolStatusParams);

        if (items.length > 0) {
            return items;
        } else {
            console.info('No Records found in Stop Dynamo Table for Order_Id:', order_id);
        }
    } catch (error) {
        console.error('Error in Stop function:', error);
        throw error;
    }
}

module.exports = {
    getMovementOrder,
    getOrder,
    updateMilestone,
    getMovement,
    getLive204OrderStatus,
    getOrderStatus,
    getConsolStatus,
    getStop
};