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
    var result = {}
    headers.forEach((key,i) => result[key.toLowerCase()] = order[i]);
    result['timestamp'] = new Date(result['time stamp']); // format as timestamp for math
    result.id = result['sequence id']; // map because the other one is annoying
    return result;
  });
  rateLimiting = {};
  formatted.forEach((order, index) => {
    if(!firms.includes(order.broker)) {
      invalid.push(order);
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
        invalid.push(order);
        return;
      }
      currentBroker.orders += 1;
    } else {
      currentBroker.currentMinute = minute;
      currentBroker.orders = 1;
    }

    if(!symbols.includes(order.symbol)) {
      invalid.push(order);
      return;
    }

    const brokerOrderIds = currentBroker.ids = currentBroker.ids || [];
    if(brokerOrderIds.includes(order.id)) {
      invalid.push(order);
      return;
    }
    currentBroker.ids.push(order.id);

    // console.log(order)
    valid.push(order)
  });
  return { valid, invalid };
}

run();

module.exports = run;
