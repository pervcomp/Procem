// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

// used to deploy the SolarUsage contract
// should be executed with the producer's account.
// Name of producer is hardcoded here.
const SolarUsage = artifacts.require("./SolarUsage.sol")

module.exports = function(deployer) {
    deployer.deploy(SolarUsage, 'solar' );
};