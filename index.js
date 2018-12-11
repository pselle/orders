const fs = require('fs');
const parse = require('csv-parse');
const { firms, symbols } = require('./resources/validators.js');

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

async function run() {
  let valid = [],
   invalid = [];
  const data = await loadData();
  const headers = data[0];
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
  rateLimiting = {};
  const invalidStream = fs.createWriteStream('invalid.csv', {flags: 'a'});
  const validStream = fs.createWriteStream('valid.csv', {flags: 'a'});
  formatted.forEach((order, index) => {
    if(!isValidBroker(order)) {
      writeToStream(invalidStream, order);
      return;
    };
    const minute = order.timestamp.getMinutes();
    const currentBroker = rateLimiting[order.broker] = rateLimiting[order.broker] || {};
    if(currentBroker.currentMinute === undefined) {
      rateLimiting[order.broker].currentMinute = minute;
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

    if(!symbols.includes(order.symbol)) {
      writeToStream(invalidStream, order);
      return;
    }

    const brokerOrderIds = currentBroker.ids = currentBroker.ids || [];
    if(brokerOrderIds.includes(order.sequence_id)) {
      writeToStream(invalidStream, order);
      return;
    }
    currentBroker.ids.push(order.sequence_id);

    writeToStream(validStream, order);
  });
}

run();

function containsNecessaryKeys(order) {
  const keys = ['broker', 'symbol', 'type', 'quantity', 'sequence_id', 'side', 'price'];
  keys.forEach((key) => {
    if(order[key] === undefined) {
      return false;
    }
  });
  return true;
}

function isValidSymbol(order) {
  return symbols.includes(order.symbol);
}

function isValidBroker(order) {
  return firms.includes(order.broker);
}

function writeToStream(stream, order) {
  stream.write(Object.values(order).join(','));
}


module.exports = { containsNecessaryKeys, isValidSymbol, isValidBroker };
