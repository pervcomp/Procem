/* A mock implementation of the ProCem RTL data collector component's get most recent measurement feature.
 * Can be used in testing rtl_sender and especially its ability to handle errors which this generates.
 */
// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta


const dgram = require( 'dgram' );

// client for UDP communication with rtl_sender
const client = dgram.createSocket('udp4');

// measurements returned are simply increments of this variable
let energy = 1;
// we save previously reported energies for each participants here so we can create the error
// where we return the same measurement we did previously i.e there is no new measurement for this
let participants = {};

// these are used in determining randomly when to generate an error of certain type
const noReply = 0.2;
const sameTime = 0.3;
const invalidMessage = 0.4;

// handle a measurement request
client.on("message", (msg, source) => {
  let message;
  msg = msg.toString().split( ':' );
  // id of measurement to be returned
  let id = msg[1];
  console.log( 'Request for ' +id );
  let participant = participants[id];
  if ( !participant ) {
    participant = {};
    participants[id] = participant;
    console.log( 'And this is the first request.' );
  }

  // random number used to determine will we give a valid reply or produce some kind of error
  let random = Math.random();
  // we check if the number is lesser than our limits for the different errors
  if ( random < noReply ) {
    console.log( 'Randomly ignore request.' );
    return;
  }

  else if ( participant.energy && participant.time && random < sameTime ) {
    // we can only do this if we have given something to this participant before
    console.log( 'Randomly pretend that we do not have a new measurement for this.' );
    // reply with message containing info we send last time
    message = new Buffer( id +';' +participant.energy +';' +participant.time );
  }

  else if ( random < invalidMessage ) {
    console.log( 'Randomly send an invalid message.' );
    message = new Buffer( 'foo' );
  }

  else {
    // lets send a valid reply
    let time = Date.now();
    message = new Buffer( id +';' +energy +';' +time );
    console.log( 'Send valid message: ' +message );

    // save energy and time we reported which we might use next time when giving the same result again as an error
    participant.energy = energy;
    participant.time = time;
    energy++; // increment energy for next reporting
  }

  // lets send our reply
  client.send(message, 0, message.length, source.port, source.address, function(err, bytes) {
    if ( err ) {
      console.log( err );
    }
  });
});

client.on( 'error', ( err ) => console.log( err ));

// listen for UDP connections on port 6667
client.bind( 6667 );