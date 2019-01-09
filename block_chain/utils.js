// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

// utility functions for working with SolarUsage contract
const contract = require( 'truffle-contract' );
const Web3 = require( 'web3' );

const utils = {

    scale: undefined, // convert energy between contract's representation and kilowat hours
    instance: undefined, // a SolarUsage instance
    web3: undefined, // web3 instance for interfacing with ethereum

    // convert participant information returned as an array of values into an object
    // utils.scale has to be set before using this
    buildParticipant: function ( values ) {
      const properties = [ 'name', 'participantType', 'latest', 'total', 'localUsage', 'reported' ];
      let result = {}
      for ( let i = 0; i < values.length; i++ ) {
        let value = values[ i ];
        if ( typeof value == 'object' ) {
          value = value.toNumber() /utils.scale; // convert to kwh
        }

        result[ properties[i]] = value;
      }

      return result;
    },

    // get a truffle contract abstraction of SolarUsage that can be used to work with the contract e.g. get an instance
    // Returns also a web3 instance used to work with given ethereum network with the given port and host via http
    // The web3 instance is set as the provider of the contract abstraction.
    // gasLimit and gasPrice are optional parameters for contract abstraction
    // returns object with contract and web3 attributes
    getContract: function ( host, port, gasLimit, gasPrice ) {
      // contract description json created by truffle compile and migrate
      var description = require( './build/contracts/SolarUsage.json' );
      // connection to ethereum
      var provider = new Web3.providers.HttpProvider( 'http://' +host +":" +port );
      var web3 = new Web3( provider );
      utils.web3 = web3;
      // truffle contract abstraction for SolarUsage
      var SolarUsage = contract( description );
      SolarUsage.setProvider( provider );

      if ( gasLimit != undefined && gasPrice != undefined ) {
        // gas limit and price for contract usage
        SolarUsage.defaults( {
          gas: gasLimit,
          gasPrice: gasPrice
        });
      }

      return { contract: SolarUsage, web3: web3 };
    },

    // get time as a string from given timestamp
    // timestamp can be a Date object or Number containing a unix timestamp in
    // milliseconds
    getTimeString: function ( timestamp ) {
      let date = undefined;
      if ( timestamp instanceof Date ) {
        date = timestamp;
      }

      else if ( typeof timestamp == 'number' ) {
        date = new Date( timestamp );
      }

      else {
        throw new Error( 'timestamp not a Number or Date object.' );
      }

      return date.toTimeString().split( " " )[0];
    },

    // convert the given SolarUsage event into a string
    // string includes event time, name and parameters except who which is an Ethereum address
    // time is either the time attribute of the event or if not added the current time
    // event parameters except name are assumed to be BigNumber instances i.e. energy amounts from contract.
    // They are converted to kilowatt hours (utils.scale) and rounded to given precision utils.eventPrecision
    // utils.scale and utils.eventPrecision have to be set before using this function
    eventToString: function ( event ) {
      // construct message to be shown
      // print event namealways
      let message = event.event;
      // if there are arguments include them except the ethereum account address
      Object.keys( event.args ).forEach( arg => {
        if ( arg != 'who' ) {
          let value = event.args[ arg ];
          if ( typeof value == 'object' ) {
            // this is a BigNumber instance get js number and scale to kwh
            value = value.toNumber() /utils.scale;
            value = utils.round( value, utils.eventPrecision );
          }

          message += ' ' +arg +': ' +value;
        }
      });

      let time = event.time;
      if ( !time ) {
        // not set use current time
        time = new Date();
      }

      // convert time to string prepent to message and return
      return utils.getTimeString( time ) +': ' +message;
    },

    // get list of all participants in the contract
    // participants are returned as objects created with buildParticipant
    // requires utils.instance to be set to a SolarUsage instance
    getAllParticipants: async function () {
      if ( !utils.addresses ) {
        // we don't yet have addresses of participants so get them first
        await utils.getAddresses();
      }

      let data = []; // get each participant here
      for ( let i = 0; i < utils.addresses.length; i++ ) {
        // build an object from the array of values we get from the contract
        let info = utils.buildParticipant( await utils.instance.participants.call( utils.addresses[i] ));
        data.push( info );
      }

      return data;
    },

    // get the public Ethereum addresses of all participants
    // also sets them as utils.addresses
    // requires utils.instance to be set to a SolarUsage instance
    getAddresses: async function () {
      let addresses = [];
      let stop = false;
      // go through participantAddresses until we get the 0 address or an exception meaning there are no more addresses
      // geth returns the 0x but ganache-cli throws an exception
      for ( let i = 0; !stop; i++ ) {
        try {
          let address = await utils.instance.participantAddresses.call( i );
          if ( address != '0x' ) {
            addresses.push( address )
          }

          else {
            stop = true;
          }
        }

        catch ( error ) {
          stop = true;
        }
      }

      utils.addresses = addresses;
      return addresses;
    },

    // Get the time everybody registered as a JavaScript Date
    // todo: should handle situation where everybody have not yet registered
    // requires utils.instance to be set to a SolarUsage instance
    getWhenRegistered: async function () {
      let timestamp = await utils.instance.everybodyRegisteredTime.call();
      timestamp = timestamp.toNumber();
      return new Date( 1000 *timestamp );
    },

    // get list containing at most given amount of events of the given type
    // list starts from the newest event
    // type can be allEvents or one of SolarUsage events
    // list is returned via the callback
    // requires utils.instance to be set to a SolarUsage instance and utils.web3 to a web3 instance
    getOldEvents: function ( amount, eventType, callback ) {
      let numBlocks = 200; // number of blocks we get at a time and search for events
      let events = []; // save found events here

      // function for getting the actual events
      // left: how many events of amount we still have to find
      // from: the block number we start searching from
      // to: block number we search to
      function getMoreEvents( left, from, to ) {
        let eventSource; // we get events from this with its get method
        // source is different depending on if we get all events or events of specific type
        if ( eventType == 'allEvents' ) {
          eventSource = utils.instance[eventType]( { fromBlock: from, toBlock: to });
        }

        else {
          eventSource = utils.instance[eventType]( {}, { fromBlock: from, toBlock: to });
        }

        eventSource.get( ( err, result ) => {
          if ( err ) {
            return callback( err );
          }

          // go through found events from newest until we have processed them all or we have enough events
          for ( let i = result.length -1; i >= 0 && events.length < amount; i-- ) {
            let event = result[i];
            // we want to have the events block time but since the event we got does not have it we have to get it separately
            utils.addTimeToEvent( event );
            events.push( event );
          }

          //console.log( result.length +' ' +from +' ' +to )
          if ( events.length >= amount ) {
            // we are done we found enough events
            callback( null, events );
          }

          else {
            // we need more events get more from next numBlocks blocks
            // or at least as many as there are left if any
            from = from -numBlocks;
            to = to -numBlocks;
            if ( from < 0 ) {
              from = 0;
            }

            if ( to < 0 ) {
              // no more blocks left, return the events we found even though we did not get the given amount
              return callback( null, events );
            }

            // try to get more events
            getMoreEvents( left -result.length, from, to );
          }
        })
      }

      // lets start from the current block
      let to = utils.web3.eth.getBlock( 'latest' ).number;
      let from = to -numBlocks +1;
      if ( from < 0 ) {
        from = 0;
      }

      getMoreEvents( amount, from, to  );
    },

    // add the block timestamp of the given event's block to the event as event.time
    // event.time will be a JavaScript Date object
    // events have only their block number so we have to get the time separately
    // utils.web3 has to be set for this to work
    addTimeToEvent: function ( event ) {
      // cache block timestamps based on block number so we don't have to allways get it
      if ( !utils.blockTimes ) {
        utils.blockTimes = {};
      }

      let blockNumber = event.blockNumber;
      let time = utils.blockTimes[ blockNumber ];
      if ( !time ) {
        // time not in cache.
        // we have to get the block corresponding to the block number
        time = new Date( utils.web3.eth.getBlock( blockNumber ).timestamp *1000 );
        utils.blockTimes[blockNumber] = time;
      }

      event.time = time;
    },

    // round the given number to given precision
    round: function ( number, precision ) {
      return number.toFixed( precision )
    }
}

module.exports = utils;