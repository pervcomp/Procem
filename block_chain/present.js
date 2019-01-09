// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

// Simple NodeJS program to demonstrate on how to get the present values from procem RTL.
// The present value is received as a response after sending a UDP message containing
// "get_value:<id_number>". The response contains both the value and the timestamp separated by ";".
//
// Example usage of this program:
//   nodejs present.js 5001 5002
//   id: 5001 (v: 23.3474, ts: 1531224245272)
//   id: 5002 (v: 25.5843, ts: 1531224245272)

const dgram = require("dgram");

const HOST = "127.0.0.1";
const PORT = 6666;

const DELAY_S = 60;
const DELAY_MS = DELAY_S *1000;

const GET_VALUE_MESSAGE = "get_value:";

const client = dgram.createSocket("udp4");
client.on("message", (msg, source) => {
  const message = msg.toString().split( ';' );
  const value = {
      id: message[0],
      "v": message[1],
      "ts": Number( message[2] )
  };
  console.log( "id: " +value.id +" (v: " + value["v"] + ", ts: " + new Date( value["ts"] ) + ")");
});

function presentValue(id) {
    const message = new Buffer(GET_VALUE_MESSAGE + id)

    client.send(message, 0, message.length, PORT, HOST, function(err, bytes) {
        if (err) {
            throw err;
        }
    });
}

function main() {
    const args = process.argv.slice(2);
    let start = new Date();
    console.log( `Starting at ${start}` );
    let wait = DELAY_MS -(start.getTime() %DELAY_MS);
    let first = new Date( start.getTime() +wait );
    console.log( `Getting first data at ${first.toTimeString()}` );
    setTimeout( () => {
      getData();
      setInterval( getData, DELAY_MS );
    }, wait );

    function getData() {
      let now = new Date();
      console.log( "sending data requests at " +now.toTimeString() );
      args.forEach(function(id) {
        presentValue(id);
      });
    }
}

main();
