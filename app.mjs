import fs from 'fs';
import moment from 'moment';
import { SQSClient, ReceiveMessageCommand }from '@aws-sdk/client-sqs';

const sqsClient = new SQSClient({region: 'us-east-1'});
const queueURL = "https://sqs.us-east-1.amazonaws.com/000000000000/Project2Queue";

// To start this fake service, simply run from the terminal:  npm start 
// To stop the service:  Press Ctrl+C, and answer Yes

const logFileName = 'Project2Service.log';

// Use this function to write to the service log file.
// Remember that you need to write to the log wvery time you:
// - Read a message from the DownwardQueue
// - Put a message in the UpwardQueue
// (See bullet point # 4 on pages 4 and 5 of Project2.pdf)
function writeToLogFile(message) {
    const timestamp = moment().format('YYYY-MM-DD hh:mm:ss')
    let content = `${timestamp}:  ${message}\n`
    fs.appendFile(logFileName, content, err => {
        if (err) {
          console.error(err);
        } 
    });
}

async function readFromDownwardQueue() {
    // TODO:
    // 1- Check for messages in the DownwardQueue
    const params = {
        QueueURL: queueURL,
        WaitTimeSeconds: 20,
        MaxNumberOfMessages: 1
    };

    const command = new ReceiveMessageCommand(params);
    const response = await sqsClient.send(command);

    console.log(response);




    // 2- Query the database for insurance info (InsuranceDatabase.xml or InsuranceDatabase.json)
    // 3- Return data to UpwardQueue
}

console.log('==>> InsuranceDataService started');
console.log('==>> Listening to incoming messages from DownwardQueue...');

// Execute the readFromDownwardQueue every 3 seconds
setInterval(readFromDownwardQueue, 3000);

