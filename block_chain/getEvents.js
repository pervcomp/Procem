/* Command line utility for listing the desired number of previous events from SolarUsage
 * usage: node getEvents.js number_of_events
 * Reads web3 http connection information from getEvents_config.json
 */
// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta


const utils = require( './utils.js' );

async function main() {
  // how many events to get from command line parameter
  let amount = Number( process.argv[2] );
  const config = require( './getEvents_config.json' );

  // get contract abstraction and connection to ethereum
  let contract = utils.getContract( config.web3_host, config.web3_port );
  // set utils contract instance and scale before using its getOldEvents
  utils.instance = await contract.contract.deployed();
  utils.scale = await utils.instance.SCALE.call();
  utils.eventPrecision = 2; // precision for showing numbers in events

  // get the events
  utils.getOldEvents( amount, 'allEvents', ( err, result ) => {
    if ( err ) {
      return console.log( err );
    }

    console.log( result.length + ' events found.' );
    result.forEach( event => console.log( utils.eventToString( event )));
  });
}

main();