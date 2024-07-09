const AWS = require('aws-sdk');
const _ = require('lodash');
const {
  getOrderStatus,
  getConsolStatus,
  consigneeIsCustomer
} = require('../shared/dynamo');
const moment = require('moment-timezone');
const axios = require('axios');
const sns = new AWS.SNS();
const { get } = require('lodash');
const { js2xml } = require('xml-js');
const dynamoDb = new AWS.DynamoDB.DocumentClient();

const {
  ENVIRONMENT,
  ERROR_SNS_TOPIC_ARN,
  ADD_MILESTONE_TABLE_NAME,
  WT_SOAP_USERNAME,
  ADD_MILESTONE_URL,
  ADD_MILESTONE_URL_2,
} = process.env;

let functionName;

let itemObj = {
  Id: '',
  OrderId: '',
  Housebill: '',
  StatusCode: '',
  EventDateTime: '',
  Payload: '',
  Reponse: '',
  ErrorMessage: '',
  Status: '',
  FK_OrderNo: '',
  ConsolNo: '',
};

module.exports.handler = async (event, context) => {
  const records = _.get(event, 'Records', []);
  for (const record of records) {
    let XMLpayLoad;
    let dataResponse;
    try {
      const newUnmarshalledRecord = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

      itemObj.Id = _.get(newUnmarshalledRecord, 'Id');
      itemObj.OrderId = _.get(newUnmarshalledRecord, 'OrderId');
      itemObj.Housebill = _.get(newUnmarshalledRecord, 'Housebill');
      itemObj.StatusCode = _.get(newUnmarshalledRecord, 'StatusCode');
      itemObj.EventDateTime = moment.tz('America/Chicago').format();
      itemObj.Payload = JSON.stringify({
        Id: itemObj.OrderId,
        Housebill: itemObj.Housebill,
        StatusCode: itemObj.StatusCode,
        EventDateTime: itemObj.EventDateTime,
      });

      console.info('Processed Item:', itemObj);

      console.info('OrderId coming from Event:', itemObj.OrderId);

      const orderStatusValidationData = await getOrderStatus(itemObj.OrderId);
      console.info('Data Coming from 204 Order Status Table:', orderStatusValidationData);

      const type = _.get(orderStatusValidationData, 'Type', '');
      itemObj.FK_OrderNo = _.get(orderStatusValidationData, 'FK_OrderNo', '');

      if (type === 'NON_CONSOLE') {
        console.info('This is of Type NON_CONSOLE');

        XMLpayLoad = await generateNonConsolXmlPayload(itemObj);
        console.info('XML Payload Generated :', XMLpayLoad);

        dataResponse = await addMilestoneApiDataForNonConsol(XMLpayLoad);
        console.info('dataResponse', dataResponse);
        itemObj.Reponse = dataResponse;

        await updateStatusTable(
          itemObj.Housebill,
          itemObj.StatusCode,
          'SENT',
          XMLpayLoad,
          dataResponse
        );
      } else if (type === 'CONSOLE') {
        console.info('This is of Type CONSOLE');

        if (itemObj.StatusCode === 'DEL') {
          const conIsCu = await consigneeIsCustomer(itemObj.FK_OrderNo, type);
          if (conIsCu) {
            console.log('send event DEL');
            itemObj.StatusCode = 'DEL';
          } else {
            itemObj.StatusCode = 'AAD';
          }
        } else if (itemObj.StatusCode === 'DWP') {
          const conIsCu = await consigneeIsCustomer(itemObj.FK_OrderNo, type);
          if (conIsCu) {
            console.log('send event DWP');
            itemObj.StatusCode = 'DWP';
          } else {
            itemObj.StatusCode = 'AAD';
          }
        }
        XMLpayLoad = await generateConsolXmlPayload(itemObj);
        console.info('XML Payload Generated :', XMLpayLoad);

        dataResponse = await addMilestoneApiDataForConsol(XMLpayLoad);
        console.info('dataResponse', dataResponse);
        itemObj.Reponse = dataResponse;

        await updateStatusTable(
          itemObj.FK_OrderNo,
          itemObj.StatusCode,
          'SENT',
          XMLpayLoad,
          dataResponse
        );
      } else {
        console.info('This is of Type Multistop');

        if (itemObj.StatusCode === 'DEL') {
          const conIsCu = consigneeIsCustomer(itemObj.ConsolNo, 'MULTISTOP');
          if (conIsCu) {
            console.log('send event DEL');
            itemObj.StatusCode = 'DEL';
          } else {
            itemObj.StatusCode = 'AAD';
          }
        } else if (itemObj.StatusCode === 'DWP') {
          const conIsCu = consigneeIsCustomer(itemObj.ConsolNo, 'MULTISTOP');
          if (conIsCu) {
            console.log('send event DWP');
            itemObj.StatusCode = 'DWP';
          } else {
            itemObj.StatusCode = 'AAD';
          }
        }

        const consolStatusValidationData = await getConsolStatus(itemObj.OrderId);
        console.info('Data Coming from 204 Consol Status Table:', consolStatusValidationData);

        itemObj.ConsolNo = _.get(consolStatusValidationData, 'ConsolNo', '');

        XMLpayLoad = await generateMultistopXmlPayload(itemObj);
        console.info('XML Payload Generated :', XMLpayLoad);

        dataResponse = await addMilestoneApiDataForConsol(XMLpayLoad);
        console.info('dataResponse', dataResponse);
        itemObj.Reponse = dataResponse;

        await updateStatusTable(
          itemObj.ConsolNo,
          itemObj.StatusCode,
          'SENT',
          XMLpayLoad,
          dataResponse
        );
      }
    } catch (error) {
      console.error('Error processing event:', error);
      await updateStatusTable(
        itemObj.Housebill,
        itemObj.StatusCode,
        'FAILED',
        XMLpayLoad,
        dataResponse,
        error.message
      );
      await publishSNSTopic({ Id: itemObj.Id, message: `Error Details:${error}` });
      throw error;
    }
  }
};

async function publishSNSTopic({ Id, message }) {
  try {
    const params = {
      TopicArn: ERROR_SNS_TOPIC_ARN,
      Subject: `PB ADD MILESTONE ERROR NOTIFICATION - ${ENVIRONMENT} ~ Id: ${Id}`,
      Message: `An error occurred in ${functionName}: ${message}`,
    };

    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}

async function generateNonConsolXmlPayload(itemObj) {
  try {
    const xml = js2xml(
      {
        'soap:Envelope': {
          '_attributes': {
            'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
            'xmlns:soap': 'http://schemas.xmlsoap.org/soap/envelope/',
          },
          'soap:Body': {
            UpdateStatus: {
              _attributes: {
                xmlns: 'http://tempuri.org/',
              },
              HandlingStation: '',
              HAWB: _.get(itemObj, 'Housebill', ''),
              UserName: WT_SOAP_USERNAME,
              StatusCode: _.get(itemObj, 'StatusCode', ''),
              EventDateTime: _.get(itemObj, 'EventDateTime', ''),
            },
          },
        },
      },
      { compact: true, ignoreComment: true, spaces: 4 }
    );
    console.info('XML payload', xml);
    return xml;
  } catch (error) {
    console.error('Error generating XML:', error);
    return null;
  }
}

async function generateConsolXmlPayload(itemObj) {
  try {
    const xml = js2xml(
      {
        'soap:Envelope': {
          '_attributes': {
            'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
            'xmlns:soap': 'http://schemas.xmlsoap.org/soap/envelope/',
          },
          'soap:Body': {
            UpdateStatusByConsolNo: {
              _attributes: {
                xmlns: 'http://tempuri.org/',
              },
              HandlingStation: '',
              ConsolNo: _.get(itemObj, 'FK_OrderNo', ''),
              UserName: WT_SOAP_USERNAME,
              StatusCode: _.get(itemObj, 'StatusCode', ''),
              EventDateTime: _.get(itemObj, 'EventDateTime', ''),
            },
          },
        },
      },
      { compact: true, ignoreComment: true, spaces: 4 }
    );
    console.info('XML payload', xml);
    return xml;
  } catch (error) {
    console.error('Error generating XML:', error);
    return null;
  }
}

async function generateMultistopXmlPayload(itemObj) {
  try {
    const xml = js2xml(
      {
        'soap:Envelope': {
          '_attributes': {
            'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
            'xmlns:soap': 'http://schemas.xmlsoap.org/soap/envelope/',
          },
          'soap:Body': {
            UpdateStatusByConsolNo: {
              _attributes: {
                xmlns: 'http://tempuri.org/',
              },
              HandlingStation: '',
              ConsolNo: _.get(itemObj, 'ConsolNo', ''),
              UserName: WT_SOAP_USERNAME,
              StatusCode: _.get(itemObj, 'StatusCode', ''),
              EventDateTime: _.get(itemObj, 'EventDateTime', ''),
            },
          },
        },
      },
      { compact: true, ignoreComment: true, spaces: 4 }
    );
    console.info('XML payload', xml);
    return xml;
  } catch (error) {
    console.error('Error generating XML:', error);
    return null;
  }
}

async function addMilestoneApiDataForNonConsol(postData) {
  try {
    const config = {
      method: 'post',
      headers: {
        'Accept': 'text/xml',
        'Content-Type': 'text/xml',
      },
      data: postData,
    };

    config.url = `${ADD_MILESTONE_URL}?op=UpdateStatus`;

    console.log('config: ', config);
    const res = await axios.request(config);
    if (get(res, 'status', '') == 200) {
      return get(res, 'data', '');
    } else {
      itemObj.xmlResponsePayload = get(res, 'data', '');
      throw new Error(`API Request Failed: ${res}`);
    }
  } catch (error) {
    const response = error.response;
    console.error('Error in addMilestoneApi', {
      message: error.message,
      response: {
        status: response?.status,
        data: response?.data,
      },
    });
    throw error;
  }
}

async function addMilestoneApiDataForConsol(postData) {
  try {
    const config = {
      method: 'post',
      headers: {
        'Accept': 'text/xml',
        'Content-Type': 'text/xml',
      },
      data: postData,
    };

    config.url = `${ADD_MILESTONE_URL_2}?op=UpdateStatus`;

    console.log('config: ', config);
    const res = await axios.request(config);
    if (get(res, 'status', '') == 200) {
      return get(res, 'data', '');
    } else {
      itemObj.xmlResponsePayload = get(res, 'data', '');
      throw new Error(`API Request Failed: ${res}`);
    }
  } catch (error) {
    const response = error.response;
    console.error('Error in addMilestoneApi', {
      message: error.message,
      response: {
        status: response?.status,
        data: response?.data,
      },
    });
    throw error;
  }
}

async function updateStatusTable(
  Housebill,
  StatusCode,
  apiStatus,
  Payload = '',
  Response = '',
  ErrorMessage = ''
) {
  try {
    const updateParam = {
      TableName: ADD_MILESTONE_TABLE_NAME,
      Key: {
        Housebill,
        StatusCode,
      },
      UpdateExpression:
        'set Payload = :payload, #Response = :response, #Status = :status, EventDateTime = :eventDateTime, ErrorMessage = :errorMessage',
      ExpressionAttributeNames: {
        '#Status': 'Status',
        '#Response': 'Response',
      },
      ExpressionAttributeValues: {
        ':payload': String(Payload),
        ':response': String(Response),
        ':status': apiStatus,
        ':eventDateTime': moment.tz('America/Chicago').format(),
        ':errorMessage': ErrorMessage,
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDb.update(updateParam).promise();
  } catch (err) {
    console.error('🙂 -> file: index.js:224 -> err:', err);
    throw err;
  }
}
