import fs from 'fs';
import moment from 'moment';
import { SQSClient, ReceiveMessageCommand, SendMessageCommand, DeleteMessageCommand }from '@aws-sdk/client-sqs';
import { fromIni } from "@aws-sdk/credential-providers";
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";
import { v4 as uuidv4 } from 'uuid';

const sqsClient = new SQSClient({
    region: 'us-east-1',
    credentials: fromIni({ profile: 'default' })
});

const queueURL = "https://sqs.us-east-1.amazonaws.com/692998282702/DownwardQueue.fifo";

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
    
    // 1- Check for messages in the DownwardQueue
    const params = {
        QueueUrl: queueURL, 
        WaitTimeSeconds: 20,
        MaxNumberOfMessages: 1
    };
    
    let response = undefined;
    let patient = undefined;
    let receiptHandle = undefined;

    try {
        const command = new ReceiveMessageCommand(params);
        response = await sqsClient.send(new ReceiveMessageCommand(params));
        if (response.$metadata.httpStatusCode != 200) {
            console.log("Error", response.$metadata.httpStatusCode);
            return;
        } 

        // Ensure that there is at least one message returned
        if (!response.Messages || response.Messages.length === 0) {
            console.log("No messages available.");
            return;
        }

        console.log("Reading from DownwardQueue...");
        patient = JSON.parse(response.Messages[0].Body);
        writeToLogFile("Read message: " + response.Messages[0].Body);
        receiptHandle = response.Messages[0].ReceiptHandle;

    } catch (err) {
        console.log("Error", err);
    }

    if (response && response.$metadata.httpStatusCode === 200) {
        // 2- Query the database for insurance info (InsuranceDatabase.xml or InsuranceDatabase.json)
        fs.readFile('/Users/lucasoliveira/Documents/code/cloud/Project2/InsuranceDatabase/InsuranceDatabase.json', 'utf8', async (err, data) => {
            if (err) {
                console.error(err);
            }
            
            if (err === null) { 
                let patients = JSON.parse(data).insuranceDatabase.patients;
                patients.forEach((dBpatient) => {
                    if (dBpatient._id === patient.id) {
                        patient.policyNumber = dBpatient.policy._policyNumber;
                        patient.provider = dBpatient.policy.provider;            
                    }
                });

                // 3- Return data to UpwardQueue
                console.log(JSON.stringify(patient, null, 2));        
                const params = {
                    QueueUrl: "https://sqs.us-east-1.amazonaws.com/692998282702/UpwardQueue.fifo",
                    MessageBody: JSON.stringify(patient, null, 2),
                    MessageGroupId: patient.id, 
                    MessageDeduplicationId: `${patient.id}-${Date.now()}-${uuidv4()}`
                };

                writeToLogFile("Posted message: " + JSON.stringify(patient, null, 2));

                const command = new SendMessageCommand(params);
                const anotherResponse = await sqsClient.send(command);
                console.log(anotherResponse.$metadata.httpStatusCode == 200 ? "Success Pushing to UpwardQueue" : "ERROR Pushing to UpwardQueue");

                // 4- Delete the processed message from the DownwardQueue
                try {
                    const deleteParams = {
                        QueueUrl: queueURL,
                        ReceiptHandle: receiptHandle
                    };
                    const deleteCommand = new DeleteMessageCommand(deleteParams);
                    const deleteResponse = sqsClient.send(deleteCommand);
                    console.log(deleteResponse.$metadata.httpStatusCode === 200 
                    ? "Successfully deleted message from DownwardQueue" 
                    : "Error deleting message from DownwardQueue");
                } catch (deleteErr) {
                    console.error("Error deleting message:", deleteErr);
                }
            }
        });
    }
}

console.log('==>> InsuranceDataService started');
console.log('==>> Listening to incoming messages from DownwardQueue...');

// Execute the readFromDownwardQueue every 3 seconds
setInterval(readFromDownwardQueue, 3000);



/**
 * Test authentication to AWS
 */
const stsClient = new STSClient({
    region: 'us-east-1',
    credentials: fromIni({ profile: 'default' })
});
  
async function testAuth() {
    try {
    const data = await stsClient.send(new GetCallerIdentityCommand({}));
    console.log('Authentication successful:');
    console.log(`Account: ${data.Account}`);
    console.log(`ARN: ${data.Arn}`);
    console.log(`UserId: ${data.UserId}`);
    } catch (err) { 
        console.error('Authentication error:', err);
    }
}
  
//testAuth();
