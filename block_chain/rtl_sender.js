/* Calculate energy for given time period from procem rtl and send to smart contract.
 */
// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

const dgram = require("dgram");
const winston = require( 'winston' );
const utils = require( './utils.js' );
const getTimeString = utils.getTimeString;

// read common configuration file
const common = require( './common_config.json' );
// connection information to ProCem data collector
const HOST = common.rtl_host || "127.0.0.1";
const PORT = common.rtl_port || 6666;
// length of the period in seconds
const DELAY_S = common.delay || 60;
// same in milliseconds used internally
const DELAY_MS = DELAY_S *1000;
// name of instance specific configuration file
const myConfName = process.argv[2];
const myConf = require( './' +myConfName );
// procem measurement id of the participants cumulative energy
const id = myConf.id;
// participant name
const name = myConf.name;

// configure logging: everything is printed to console and information and warnings to file
const logger = winston.createLogger( {
  transports: [
    new winston.transports.Console( { level: 'verbose', format: winston.format.simple() }),
    new winston.transports.File( {
      level: 'info',
      filename: name +'.log',
      format: winston.format.combine(
          winston.format.timestamp(),
          winston.format.printf( info => {
            return `${info.timestamp}\n${info.level}: ${info.message}`
          })
        )
    })
  ]
});

// common part of message send to datacollector for requesting latest value for a measurement
const GET_VALUE_MESSAGE = "get_value:";

// client for udp communication with data collector
const client = dgram.createSocket("udp4");

// keep measurement gotten from previous round in here
let previousValue = undefined;
// SolarUsage instance for interacting with the contract
let instance = undefined;
// ethereum address of this participant
let address = undefined;
// factor used in converting kilowatt hours for the contract
let scale = undefined;

// timer set when sending data request in case we do not get a response and have to try again after awhile
let getDataRetry = undefined;

// get the time in milliseconds how long to wait before next energy value should be fetched
function getWaitTime() {
  return DELAY_MS -((new Date()).getTime() %DELAY_MS);
}

// process measurement requested from data collector
client.on("message", async (msg, source) => {
  if ( !getDataRetry ) {
    // we had not set a timer for retrying getting the data so we did not expect this
    // message so we will ignore it
    return logger.warn( 'Got extra data package that will not be processed. ' +getTimeString( Date.now() ));
  }

  // we got data after requesting it.
  // We can clear retry timer since we don't have to try again to get the data
  clearInterval( getDataRetry );
  // mark the timer as undefined so we know that we are not trying to get data now
  getDataRetry = undefined;

  let value; // the new data we got
  try {
    // message should be id, value and timestamp separated by ;
    const message = msg.toString().split( ';' );
    value = {
        id: Number( message[0] ),
        // there is an error in measurements so we have to double the value
        v: 2 *Number( message[1] ),
        ts: Number( message[2] )
    };

    if ( id !== value.id ) {
      throw new Error( 'No id or wrong id in message.' );
    }

    if ( isNaN( value.v ) || isNaN( value.ts )) {
      throw new Error( 'Invalid value or timestamp.' );
    }
  }

  catch ( error ) {
    logger.warn( 'Got invalid message from procem rtl: ' +error.message );
    logger.warn( 'Message was: ' +msg );
    logger.warn( 'Attempting to get new value after 3 seconds.' );
    return setTimeout( getData, 3000 );
  }

  logger.verbose( "Got data: id: " +value.id +" (v: " + value["v"] + ", ts: " + new Date( value["ts"] ) + ")");
  if ( previousValue == undefined ) {
    logger.verbose( "Got first value for " +name );
    // get next data at start of next period
    setTimeout( getData, getWaitTime() );
  }

  else {
    if ( previousValue.ts == value.ts ) {
      logger.warn( 'New measurement data has the same timestamp than the previous one.');
      logger.warn(  'New measurement: ' +value.id +" (v: " + value["v"] + ", ts: " + new Date( value["ts"] ) + ")" );
      logger.warn(  'Previous measurement: ' +previousValue.id +" (v: " + previousValue["v"] + ", ts: " + new Date( previousValue["ts"] ) + ")" );
    }

    // calculate energy for the current period
    let energy = value.v -previousValue.v;
    logger.verbose( energy +" kWh energy for " +name +" between " +getTimeString( previousValue.ts ) +" - " +getTimeString( value.ts ) );
    logger.verbose( 'Reporting energy amount to contract ' +getTimeString( Date.now() ));
    let success = false;
    // we will try to report until we succeed
    while ( !success ) {
      try {
        // report energy to contract scaled from kilowat hours to contracts internal format
        await instance.reportEnergy( scale *energy, { from: address });
        logger.verbose( 'Reported successfully. ' +getTimeString( Date.now() ));
        success = true;
        // set timer for getting next data in the start of next period
        setTimeout( getData, getWaitTime() );
      }

      catch ( error ) {
        logger.warn( 'Failed to report: ' +error.message );
        logger.warn( 'Retrying to report. ' +getTimeString( Date.now() ));
      }
    }
  }

  // save the new value so that we can use it next time to calculate the energy
  previousValue = value;
});

// send data request for getting the latest value for measurement with the given id
function presentValue(id) {
    // message to be send to data collector via udp
    const message = new Buffer(GET_VALUE_MESSAGE + id)

    client.send(message, 0, message.length, PORT, HOST, function(err, bytes) {
        if (err) {
            throw err;
        }
    });
}

// function that gets the cumulative energy for current period
// this is meant to be used with a timer
function getData() {
  let now = new Date();
  if ( !getDataRetry ) {
    // this is first time we are getting data for a period
    // in case we do not get any reply we will set another timer for this
    // if we get data the message handler will cancel the timer
    logger.verbose( "sending data request at " +getTimeString( now ) );
    getDataRetry = setInterval( getData, 3000 );
  }

  else {
    // we already had a timer so this is a retry
    logger.warn( "Retrying data request at " +getTimeString( now ) );
  }

  // call function that actually sends the message to data collector
  presentValue(id);
}

// function that starts the system up
async function main() {
  // get abstract contract representation using the given ethereum connection
  // use the given gasLimit and gasPrice to ensure our transactions go through
  let contract = utils.getContract( myConf.web3_host, myConf.web3_port, common.gasLimit, common.gasPrice );
  let SolarUsage = contract.contract; // contract absrraction
  let web3 = contract.web3; // our web3 (connection to ethereum)
  // participant's address if one of the addresses managed by our ethereum node
  // configuration should define which address of them it is
  address = web3.eth.accounts[ myConf.address_index ];
  logger.info( 'Public ethereum address of ' +myConf.name +' is ' +address );
  try {
    logger.info( "Getting an instance of SolarUsage smart contract.");
    instance = await SolarUsage.deployed();
    logger.info( 'Got SolarUsage instance at ' +instance.address );
    scale = (await instance.SCALE.call()).toNumber();
    if ( myConf.registered ) {
      logger.info( myConf.name +' already registered to contract. No need to do it now.' );
    }

    else {
      logger.info( 'Registering ' +myConf.name +' to SolarUsage as consumer.' );
      await instance.registerAsConsumer( myConf.name, { from: address });
      logger.info( 'Registration successful' );
    }
  }

  catch ( error ) {
    logger.error( error.message );
    return;
  }

    let start = new Date();
    logger.info( `Preparing to start collection of measurements from procem rtl  at ${start}` );
    // how long to wait for begining of first period e.g. next full hour or minute
    let wait = getWaitTime();
    let first = new Date( start.getTime() +wait );
    logger.info( `Getting first data at ${getTimeString( first)}` );
    // set timer for getting the first energy measurement
    setTimeout( getData, wait );
}

main();