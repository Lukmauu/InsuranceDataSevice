import fs from 'fs/promises';
import moment from 'moment';
import { 
  SQSClient, 
  ReceiveMessageCommand, 
  SendMessageCommand, 
  DeleteMessageCommand 
} from '@aws-sdk/client-sqs';
import { fromIni } from "@aws-sdk/credential-providers";
import { randomUUID } from 'crypto';
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";

const sqsClient = new SQSClient({
    region: 'us-east-1',
    credentials: fromIni({ profile: 'default' })
});

// Queue URLs and file paths
const downwardQueueURL = "https://sqs.us-east-1.amazonaws.com/692998282702/DownwardQueue.fifo";
const upwardQueueURL   = "https://sqs.us-east-1.amazonaws.com/692998282702/UpwardQueue.fifo";
const logFileName = 'Project2Service.log';
const insuranceDatabaseFilePath = '/Users/lucasoliveira/Documents/code/cloud/Project2/InsuranceDatabase/InsuranceDatabase.json';

/**
 * This is a global flag to prevent overlapping processing cycles
 */  
let isProcessing = false;

/**
 * Append a log message to the log file with a timestamp.
 */
async function writeToLogFile(message) {
    const timestamp = moment().format('YYYY-MM-DD hh:mm:ss');
    const content = `${timestamp}: ${message}\n`;
    try {
        await fs.appendFile(logFileName, content);
    } catch (error) {
        console.error("Error writing log:", error);
    }
}

/**
 * Receive a message from the DownwardQueue.
 * Returns the message object or null if no message.
 */
async function receiveMessage() {
    const params = {
        QueueUrl: downwardQueueURL,
        WaitTimeSeconds: 20,
        MaxNumberOfMessages: 1
    };

    try {
        const response = await sqsClient.send(new ReceiveMessageCommand(params));

        if (!response.Messages || response.Messages.length === 0) {
            return null;
        }
    
        return response.Messages[0];
    } catch (error) {
        console.error("Error receiving message:", error);

        return null;
    }
}

/**
 * Delete a message from the DownwardQueue given its receipt handle.
 */
async function deleteMessage(receiptHandle) {
    const params = {
      QueueUrl: downwardQueueURL,
      ReceiptHandle: receiptHandle
    };
    
    try {
      const response = await sqsClient.send(new DeleteMessageCommand(params));

      return response.$metadata.httpStatusCode === 200;
    } catch (error) {
        console.error("Error deleting message:", error);
        
        return false;
    }
}

/**
 * Read and parse the insurance database JSON file and update the patient object.
 */
async function updatePatientInsurance(patient) {
    try {
        const data = await fs.readFile(insuranceDatabaseFilePath, 'utf8');
        const dbPatients = JSON.parse(data).insuranceDatabase.patients;
        for (const dbPatient of dbPatients) {
            if (dbPatient._id === patient.id) {
                patient.policyNumber = dbPatient.policy._policyNumber;
                patient.provider = dbPatient.policy.provider;
                break;
            }
        }

        return patient;
    } catch (error) {
        console.error("Error reading insurance database:", error);

        return patient; // Return unmodified patient if error occurs.
    }
}

/**
 * Send the patient object to the UpwardQueue.
 */
async function sendToUpwardQueue(patient) {
    const params = {
        QueueUrl: upwardQueueURL,
        MessageBody: JSON.stringify(patient, null, 2),
        MessageGroupId: patient.id, 
        MessageDeduplicationId: `${patient.id}-${Date.now()}-${randomUUID()}`
      };
    
      try {
        const response = await sqsClient.send(new SendMessageCommand(params));
        return response.$metadata.httpStatusCode === 200;
      } catch (err) {
        console.error("Error sending message to UpwardQueue:", err);
        return false;
      } 	
}

/**
 * Process a single message: receive, log, update with insurance info, send upward, and delete.
 */
async function processMessage() {
    const message = await receiveMessage();
    if (!message) {
        console.log("No message available...");
    
        return;
    }
  
    // Parse the patient info and log the message.
    let patient;
    try {
        patient = JSON.parse(message.Body);
    } catch (err) {
        console.error("Error parsing message:", err);
        
        return;
    }
    await writeToLogFile("Read message: " + message.Body);
    console.log("Processing patient:", patient);
  
    // Update the patient object using the insurance database.
    patient = await updatePatientInsurance(patient);
  
    // Send the updated patient data to the UpwardQueue.
    const sent = await sendToUpwardQueue(patient);
    if (sent) {
        await writeToLogFile("Posted message: " + JSON.stringify(patient, null, 2));
        console.log("Successfully pushed to UpwardQueue");
    } else {
        console.error("Error pushing message to UpwardQueue");
    
        return;
    }
  
    // Delete the processed message from the DownwardQueue.
    const deleted = await deleteMessage(message.ReceiptHandle);
    if (deleted) {
        console.log("Successfully deleted message from DownwardQueue");
    } else {
        console.error("Error deleting message from DownwardQueue");
    }
}

/**
 * Poll the DownwardQueue. If processing is already in progress, skip this interval.
 */
async function pollQueue() {
    if (isProcessing) {
        console.log("Listining...");
     
        return;
    }

    isProcessing = true;
    try {
        await processMessage();
    } catch (error) {
        console.error("Error processing message:", error);
    } finally {
        isProcessing = false;
    }
}

console.log('==>> InsuranceDataService started');
console.log('==>> Listening to incoming messages from DownwardQueue...');

pollQueue();
setInterval(pollQueue, 20000);

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
