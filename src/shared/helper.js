'use strict';
const _ = require('lodash');
const { getConsolStopHeader, getShipmentForSeq } = require('./dynamo');

const types = {
  CONSOL: 'CONSOLE',
  MULTISTOP: 'MULTI-STOP',
};

async function getShipmentForStop({ consolNo, stopSeq }) {
  const cshRes = await getConsolStopHeader({ consolNo, stopSeq });
  const stopId = _.get(cshRes, '[0].PK_ConsolStopId');
  if (!stopId) return [];
  return await getShipmentForSeq({ stopId });
}

const milestones = {
  BOO: 'BOO',
  APP: 'APP',
  APD: 'APD',
  APL: 'APL',
  TLD: 'TLD',
  COB: 'COB',
  TTC: 'TTC',
  AAD: 'AAD',
  DWP: 'DWP',
  DEL: 'DEL'
}
module.exports = {
  types,
  getShipmentForStop,
  milestones
};
