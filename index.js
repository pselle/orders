const fs = require('fs');
const parse = require('csv-parse');
const { brokers, symbols, brokerTracking } = require('./resources/validators.js');

const parser = parse({ delimiter: ','})

function validateOrder(order) {
  const required = ['broker', 'symbol', 'type', 'quantity', 'sequence_id', 'side', 'price'];
}

function loadData() {
  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream('./resources/trades.csv');
    var data = []
    parser.on('readable', function(){
      let record
      while (record = parser.read()) {
        data.push(record);
      }
    })
    parser.on('error', function(err){
      reject(err);
    })
    parser.on('end', function(){
      resolve(data);
    })

    fileStream.pipe(parser);
  });
}

const invalidStream = fs.createWriteStream('invalid.csv', {flags: 'a'});
const validStream = fs.createWriteStream('valid.csv', {flags: 'a'});

async function run() {
  const data = await loadData();
  // slice ditches the headers
  const formatted = data.slice(1).map((order) => {
    return {
      timestamp: new Date(order[0]),
      broker: order[1],
      sequence_id: order[2],
      type: order[3],
      symbol: order[4],
      quantity: order[5],
      price: order[6],
      side: order[7]
    };
  });
  processRecords(formatted);
}

run();

function processRecords(records) {
  records.forEach((order) => {
    if(!containsNecessaryKeys(order) || !isValidBroker(order) || !isValidSymbol(order)) {
      writeToStream(invalidStream, order);
      return;
    }

    // Calculate if order is valid for the broker: it must not be >3rd
    // order in last minute, and its id must also be unique based on other orders
    // filed by that broker
    const minute = order.timestamp.getMinutes();
    const currentBroker = brokerTracking[order.broker];
    if(currentBroker.currentMinute === null) {
      currentBroker.currentMinute = minute;
      currentBroker.orders = 1;
    }
    if(currentBroker.currentMinute === minute) {
      if(currentBroker.orders === 3) {
        writeToStream(invalidStream, order);
        return;
      }
      currentBroker.orders += 1;
    } else {
      currentBroker.currentMinute = minute;
      currentBroker.orders = 1;
    }

    if(currentBroker.ids.includes(order.sequence_id)) {
      writeToStream(invalidStream, order);
      return;
    }
    currentBroker.ids.push(order.sequence_id);

    writeToStream(validStream, order);
  });
}

function containsNecessaryKeys(order) {
  const keys = ['broker', 'symbol', 'type', 'quantity', 'sequence_id', 'side', 'price'];
  for(var i = 0; i < keys.length; i++) {
    if(order[keys[i]] === undefined) {
      return false;
    }
  }
  return true;
}

function isValidSymbol(order) {
  return symbols.includes(order.symbol);
}

function isValidBroker(order) {
  return brokers.includes(order.broker);
}

function writeToStream(stream, order) {
  stream.write(`${order.broker}, ${order.sequence_id}\n`);
}


module.exports = { containsNecessaryKeys, isValidSymbol, isValidBroker };
