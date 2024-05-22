const sns = new AWS.SNS();

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

module.exports = {
    publishSNSTopic
};