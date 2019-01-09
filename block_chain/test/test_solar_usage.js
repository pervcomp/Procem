// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

// tests for SolarUsage contract
const SolarUsage = artifacts.require( 'SolarUsage' );
var bignumber = require( 'bignumber.js' );

contract('SolarUsage', async function (accounts) {
  let producerAddr = accounts[0];
  // represent the expected state of each participant
  let expectedProducer = {
    address: producerAddr,
    name: 'solar',
    participantType: 1,
    total: 0,
    latest: 0,
    localUsage: 0,
    reported: false
  };

  let expectedConsumer1 = {
    address: accounts[1],
    name: 'lift1',
    participantType: 0,
    total: 0,
    latest: 0,
    localUsage: 0,
    reported: false
  };

  let expectedConsumer2 = {
    address: accounts[2],
    name: 'lift2',
    participantType: 0,
    total: 0,
    latest: 0,
    localUsage: 0,
    reported: false
  };

  it("should have 3 total participants.", async function () {
    // get representation of deployed contract
    let instance = await SolarUsage.deployed();
    let total = await instance.PARTICIPANT_TOTAL.call();
    assert.equal( total.toNumber(), 3 );
  });

  it( 'Should have producer information.', async function() {
    let instance = await SolarUsage.deployed();
    let producer = await instance.producer.call();
    assert.equal( producer, producerAddr );
    // participant information is an array convert it to object
    let producerInfo = buildParticipant( await instance.participants.call( producer ) );
    // check that properties are as expected
    checkParticipant( producerInfo, expectedProducer );
    // address of producer should be first in participant addresses
    producer = await instance.participantAddresses.call( 0 );
    assert.equal( producer, producerAddr );
  });

  it( 'Should not let producer to report before others have registered.', async function () {
    let instance = await SolarUsage.deployed();
    let exception = false;
    try {
      await instance.reportEnergy( 1, { from: producerAddr } );
    }

    catch ( error ) {
      exception = true;
    }

    assert.isTrue( exception, 'Should have thrown exception');
  });

  let index = 1;
  [ expectedConsumer1, expectedConsumer2 ].forEach( async function( expected ) {
    it( 'should register ' +expected.name, async function () {
      let instance = await SolarUsage.deployed();
      let result = await instance.registerAsConsumer( expected.name, { from: expected.address } );
      let consumer = await instance.participants.call( expected.address );
      checkParticipant( buildParticipant( consumer ), expected );
      let address = await instance.participantAddresses.call( index );
      index++;
      assert.equal( address, expected.address, 'consumer address' );
      let total = await instance.PARTICIPANT_TOTAL.call();
      if ( total == index ) {
        assert.lengthOf( result.logs, 1, 'Should have a event.' );
        assert.equal( result.logs[0].event, 'EverybodyRegistered', 'event type' );
      }
    });
  });

  it( 'Should not allow more registrations.', async function() {
    let instance = await SolarUsage.deployed();
    let exception = false;
    try {
      await instance.registerAsConsumer( 'foo', { from: accounts[3] } );
    }

    catch ( error ) {
      exception = true;
    }

    assert.isTrue( exception, 'Register transaction should throw error.' );
  });

  it( 'Sould not allow non member to report.', async function () {
    let instance = await SolarUsage.deployed();
    await testError( instance.reportEnergy( 1, { from: accounts[3] } ));
  });

  it( 'Should be able to  report.', async function () {
    let instance = await SolarUsage.deployed();
    // energy amount each participant should report
    let amounts = {
      solar: 20,
      lift1: 10,
      lift2: 30
    };

    // report twice i.e 2 rounds
    for ( let i = 0; i < 2; i++ ) {
      await report( instance, expectedProducer, amounts['solar'] );
      // check that cannot report again in this round
      await testError( instance.reportEnergy( 1, { from: producerAddr } ));
      await report( instance, expectedConsumer1, amounts['lift1'] );
      await report( instance, expectedConsumer2, amounts['lift2'], true );
    }
  });

  it( 'should have calculated usage correctly', async function () {
    let instance = await SolarUsage.deployed();
    // what we expect after two rounds
    expectedProducer.total = 40;
    expectedProducer.localUsage = 40;
    expectedConsumer1.total = 20;
    expectedConsumer1.localUsage = 10;
    expectedConsumer2.total = 60;
    expectedConsumer2.localUsage = 30;

    let participants =  [ expectedProducer, expectedConsumer1, expectedConsumer2 ];
    // in addition to checking each participant we will also print some of their info as a table
    let data = []
    for ( let i = 0; i < participants.length; i++ ) {
      let info = buildParticipant( await instance.participants.call( participants[i].address ));
      data.push( info );
      // check that participant is as expected
      checkParticipant( info, participants[i] );
    }

    // print the following properties of each participant as a table
    // each property is a row in table
    [ 'name', 'total', 'localUsage' ].forEach( property => {
      let row = property;
      data.forEach( info => row += ' ' +info[property] );
      console.log( row );
    });
  });

  it( 'Should handle round with no energy consumption.', async () => {
    let instance = await SolarUsage.deployed();
    await report( instance, expectedProducer, 1 );
    await report( instance, expectedConsumer1, 0 );
    await report( instance, expectedConsumer2, 0, true );
  });
});

// when getting a participant from the contract an aray of values is just returned
// this function converts that array into an object
function buildParticipant( values ) {
  const properties = [ 'name', 'participantType', 'latest', 'total', 'localUsage', 'reported' ];
  let result = {};
  for ( let i = 0; i < values.length; i++ ) {
    result[ properties[i]] = values[ i];
  }

  return result;
}

// check that the given participant has the same values as the given expected participant
function checkParticipant( participant, expected ) {
  assert.equal( participant.name, expected.name, 'name' );
  assert.equal( participant.participantType.toNumber(), expected.participantType, 'type' );
  assert.equal( participant.latest.toNumber(), expected.latest, 'latest' );
  assert.equal( participant.total.toNumber(), expected.total, 'Total' );
  assert.equal( participant.localUsage.toNumber(), expected.localUsage, 'local usage' );
  assert.equal( participant.reported, expected.reported, 'reported' );
}

// tests that the given promise gives an error
async function testError( resultPromise ) {
  let exception = false;
  try {
    await resultPromise;
  }

  catch ( error ) {
    exception = true;
  }

  assert.isTrue( exception, 'Did not throw exception.');
}

// report the given participants energy amount to the contract instance
// parameter calculated tells if after reporting we should expect the contract to calculate the energy usage i.e. this was last report for the period
async function report( instance, participant, amount, calculated ) {
  // we will also check that latestTotalUsed works as expected
  let beforeTotal = await instance.latestTotalUsed.call();
  let result = await instance.reportEnergy( amount, { from: participant.address });
  printLog( result.logs );
  let afterTotal = await instance.latestTotalUsed.call();
  //console.log( beforeTotal.toNumber(), afterTotal.toNumber() );

  if ( calculated ) {
    assert.isTrue( afterTotal.eq( bignumber( 0 ) ), 'latestTotalUsed should be 0 but was ' +afterTotal.toString() );
  }

  else if ( participant.participantType == 1 ) {
    assert.isTrue( afterTotal.eq( beforeTotal ), 'latestTotalUsed before and after should be same but were ' +beforeTotal.toString() +' ' +afterTotal.toString() );
  }

  else {
      assert.isTrue( afterTotal.eq( beforeTotal.plus( amount )), 'latestTotalUsed should have increased by ' +amount + ' but before and after were ' +beforeTotal.toString() +' ' +afterTotal.toString() );
  }
}

// print events from transaction log
function printLog( log ) {
  log.forEach( item => {
    let message = item.event; // print at least event type
    // show also event arguments except account address
    Object.keys( item.args ).forEach( arg => {
      if ( arg != 'who' ) {
        message += ' ' +arg +':' +' ' +item.args[ arg ];
      }
    });

    console.log( 'event: ' +message );
  });
}