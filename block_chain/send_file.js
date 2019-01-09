/* Send energy data from file to SolarUsage contract.
 * Can be used in testing the contract and SolarUsage monitor
 * Reads Ethereum connection information and file to be used from send_file_config.json
 */
// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

const energyFileReader = require( './energyFileReader' );
const utils = require( './utils.js' );

const config = require( './send_file_config.json' );

// function that sends the stuff
async function main() {
  // get contract abstraction and web3 instance
  let contract = utils.getContract( config.web3_host, config.web3_port, 2000000, 20000000000 );
  let SolarUsage = contract.contract;
  let web3 = contract.web3;
  // associate participant id from text file with ethereum account address
  const participants = {
    au: web3.eth.accounts[0], // solar plant
    h1: web3.eth.accounts[1], // elevator 1
    h2: web3.eth.accounts[2] // elevator 2
  };

  try {
    // get representation of already deployed contract
    let instance = await SolarUsage.deployed();
    // get factor used to scale kilowatt hours before they are reported to contract
    let result = await instance.SCALE.call();
    const SCALE = result.toNumber();
    // register consumers
    let register1 = await instance.registerAsConsumer( 'elevator1', { from: participants.h1 });
    let register2 = await instance.registerAsConsumer( 'elevator2', { from: participants.h2 });

    // reads the file in to  list of objects
    energyFileReader( config.file,  SCALE, async ( err, lines ) => {
      if ( err ) {
        return console.log( err );
      }

      // send information in each line to the contract
      for ( let i in lines ) {
        let line = lines[i];
        let name = line.name;
        let amount = line.amount;
        console.log( `${name} ${amount}`);
        try {
          // report energy to contract
          let reportResult = await instance.reportEnergy( amount, { from: participants[name] });
          console.log( 'done' );
        }

        catch ( error ) {
          console.log( error.message );
        }
      }
    });
  }

  catch ( error ) {
    console.log( error );
  }
}

main();