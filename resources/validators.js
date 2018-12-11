const brokers = ['Fidelity',
              'Charles Schwab',
              'Edward Jones',
              'Ameriprise Financial',
              'TD Ameritrade',
              'Raymond James Financial',
              'AXA Advisors',
              'LPL Financial',
              'National Planning Corporation',
              'Wells Fargo AdvisorsWaddell & Reed',
              'Transamerica Financial'];

const symbols = ['BARK',
                'CARD',
                'HOOF',
                'LOUD',
                'GLOO',
                'YLLW',
                'BRIC',
                'KRIL',
                'LGHT',
                'VELL'];

const brokerTracking = brokers.reduce((acc, curr) => {
  acc[curr] = {
    currentMinute: null,
    orders: 0,
    ids: []
  }
  return acc;
}, {});

module.exports = { brokers, symbols, brokerTracking };
