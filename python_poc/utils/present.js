// -*- coding: utf-8 -*-
//
// Simple NodeJS program to demonstrate on how to get the present values from procem RTL.
// The present value is received as a response after sending a UDP message containing
// "get_value:<id_number>". The response contains both the value and the timestamp separated by ";".
//
// Example usage of this program:
//   nodejs present.js 5001 5002
//   id: 5001 (v: 23.3474, ts: 1531224245272)
//   id: 5002 (v: 25.5843, ts: 1531224245272)

// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta


const dgram = require("dgram");

const HOST = "127.0.0.1";
const PORT = 6666;

const GET_VALUE_MESSAGE = "get_value:";

function presentValue(id) {
    const client = dgram.createSocket("udp4");
    const message = new Buffer(GET_VALUE_MESSAGE + id)

    client.send(message, 0, message.length, PORT, HOST, function(err, bytes) {
        if (err) {
            throw err;
        }
        client.on("message", (msg, source) => {
            const message = msg.toString();
            const index = message.indexOf(";");
            const value = {
                "v": message.substr(0, index),
                "ts": message.substr(index + 1)
            };
            console.log("id: " + id + " (v: " + value["v"] + ", ts: " + value["ts"] + ")");
            client.close();
        });
    });
}

function main() {
    const args = process.argv.slice(2);
    args.forEach(function(id) {
        presentValue(id);
    });
}

main();
