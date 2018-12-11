const { containsNecessaryKeys, isValidSymbol } = require('.');

describe('containsNecessaryKeys', () => {
  const validOrder =  {
        broker: 'Wells Fargo Advisors',
        sequence_id: '100',
        type: '2',
        symbol: 'YLLW',
        quantity: '100',
        price: '39.12',
        side: 'Sell'
  };
  test('Only orders that have values for the required fields should be accepted', () => {
    const invalidOrder = {
      broker: 'Wells Fargo Advisors',
      sequence_id: '100',
      type: '2',
      quantity: '100',
      price: '39.12',
      side: 'Sell'
    };
    expect(containsNecessaryKeys(invalidOrder)).toBeFalsy();
    expect(containsNecessaryKeys(validOrder)).toBeTruthy();
  });
});

describe('isValidSymbol', () => {
  test('Only orders for symbols actually traded on the exchange should be accepted', () => {
    const invalidOrder = {
      broker: 'Wells Fargo Advisors',
      sequence_id: '100',
      type: '2',
      symbol: 'FAKE',
      quantity: '100',
      price: '39.12',
      side: 'Sell'
    };
    expect(isValidSymbol(invalidOrder)).toBeFalsy();
    expect(isValidSymbol(validOrder)).toBeTruthy();
  });
});
