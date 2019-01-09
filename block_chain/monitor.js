/* Monitor events from SolarUsage contracts and print them to console
 * After each round print participant's information
 * Reads http web3 host and port from monitor_config.json
 */
// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

const contract = require( 'truffle-contract' );
const utils = require( './utils.js' );
const buildParticipant = utils.buildParticipant;
const getTimeString = utils.getTimeString;

const config = require( './monitor_config.json' );

// does everything
async function main() {
  // get connection to Ethereum and contract from utils
  let contract = utils.getContract( config.web3_host, config.web3_port );
  let SolarUsage = contract.contract;
  let web3 = contract.web3;
  try {
    // get representation of deployed contract
    let instance = await SolarUsage.deployed();
    // get scale used to convert between kilowatt hours and units used in the contract
    const SCALE = await instance.SCALE.call();
    // some functions in utils requires these to be set
    utils.scale = SCALE;
    utils.instance = instance;
    utils.eventPrecision = 4; // how precisely numbers in events are shown

    // function for handling events
    async function showEvent( error, result ) {
      if ( error ) {
        return console.log( error );
      }

      console.log( utils.eventToString( result ));
    }

    // register the function as listener for all events of the contract
    instance.EverybodyRegistered().watch( showEvent );
    instance.ProducedEnergy().watch( showEvent );
    instance.ConsumedEnergy().watch( showEvent );
    instance.UsedSolar().watch( showEvent );
    instance.RoundCompleted().watch( showEvent );
    instance.RoundCompleted().watch( async () => {
      // after round completed show each participants information
      let data = await utils.getAllParticipants();
      // show the following properties of each participant as a table
      [ 'name', 'total', 'localUsage' ].forEach( property => {
        // table row to be printed
        let row = property;
        // get value from each participant for the row
        data.forEach( info => row += ' ' +info[property] );
        console.log( row );
      });
    });
  }

  catch ( error ) {
    console.log( error.message );
  }
}

main();