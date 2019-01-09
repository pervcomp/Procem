/* Reads a file containing SolarUsage contract test data in to list of objects
 * Used in send_file.js a test program for SolarUsage
 */
// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

const readline = require('readline');
const fs = require( 'fs' );

// parameters:
// file: name of the file to be read
// scale: factor used to scale kilowatt hours to contract
// callback: function called when the list is ready
module.exports = function ( file, SCALE, callback ) {
  // read all lines from file to array.
  const rl = readline.createInterface( {
    input: fs.createReadStream( file ),
    crlfDelay: Infinity
  });

  let lines = [];
  rl.on( 'line', line => {
    line = line.split( ' ' );
    // get id and energy value from line
    let name = line[0];
    let amount = SCALE *Number( line[1] );

    lines.push( { name: name, amount: amount } );
  });

  // when all lines read we are done
  rl.on( 'close', () => callback( null, lines));
  rl.on( 'error', err => callback( err ));
}