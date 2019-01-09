// Original work Copyright (c) Truffle 2018.
// Modified work Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

// Allows us to use ES6 in our migrations and tests.
require('babel-register');

module.exports = {
  // See <http://truffleframework.com/docs/advanced/configuration>
  // to customize your Truffle configuration!
    networks: {
      ganache_cli: {
        host: '127.0.0.1',
        port: 8545,
        network_id: '*'
      }
    }
};