import fs from 'fs';
import moment from 'moment';

// To start this fake service, simply run from the terminal:  npm start 
// To stop the service:  Press Ctrl+C, and answer Yes

const logFileName = 'Project2Service.log'

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
    // 2- Query the database for insurance info (InsuranceDatabase.xml or InsuranceDatabase.json)
    // 3- Return data to UpwardQueue
}

console.log('==>> InsuranceDataService started');
console.log('==>> Listening to incoming messages from DownwardQueue...');

// Execute the readFromDownwardQueue every 3 seconds
setInterval(readFromDownwardQueue, 3000);

