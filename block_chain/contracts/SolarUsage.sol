pragma solidity ^0.4.24; // define version of solidity to be used

// Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

// smart contract for recording the usage of solar power by its consumers
contract SolarUsage {

  // predefined number of participants including 1 producer and consumers
  uint8 constant public PARTICIPANT_TOTAL = 3;

  // scaling factor used to scale energy from contract's representation to kilowatt hours
  uint public constant SCALE = 1000000;

  // type of participants
  enum ParticipantType { Consumer, Producer }

  // define what information about a participant (consumer / producer) is recorded.
  struct Participant {
    string name; // participant name for human convenience
    ParticipantType participantType; // consumer / producer
    // energy usage / production from the current period
    uint latest;
    // total energy usage / production
    uint total;
    // for producer how much of total was used by consumer participants
    // for consumer how much power from producer it has used
    uint localUsage;
    // has the participant yet reported its energy for the current period
    bool reported;
  }

  // participants mapped by their ethereum account i.e. public address
  mapping( address => Participant ) public participants;

  // list of participant addresses
  // note this is required since you cannot get list of keys from above mapping
  address[] public  participantAddresses;
  // address of the producer so you don't have to find it by using the address list and mapping
  address public producer;
  // tells if everybody have registered or not i.e. can participants start to report their energy
  bool public isEverybodyRegistered = false;
  // timestamp for when everybody did register
  // tells for which moment forward the contract contains information
  uint public everybodyRegisteredTime;

  // how many of the participants have reported their energy for the current period
  uint8 public reportedNum = 0;
  // how much energy has been used by the consumers in this period
  uint public latestTotalUsed = 0;

 // define events emitted by functions

 // the permitted number of consumers i.e. PARTICIPANT_TOTAL -1 have registered
  event EverybodyRegistered();
  // the producer has produced energy for this period
  event ProducedEnergy( address who, string name, uint amount );
  // a consumer has consumed energy for this period
  event ConsumedEnergy( address who, string name, uint amount );
  // for this period a consumer has used the amount of solar energy and its total energy usage for the period was ofTotal
  event UsedSolar( address who, string name, uint amount, uint ofTotal );
  // everybody have reported their energy for this period and the usage has been calculated
  event RoundCompleted();

  // define modifiers which can be used to restrict when functions can be called

  // function can be used only if everybody have not yet registered
  modifier everybodyNotRegistered {
    require( participantAddresses.length < PARTICIPANT_TOTAL, "Everybody have already registered." );
    _;
  }

  // function can be called only if everybody have already registered
  modifier everybodyRegistered() {
    require( participantAddresses.length == PARTICIPANT_TOTAL, "Everybody have not yet  registered." );
    _;
  }

  // function can be called only by a participant
  modifier isParticipant {
     // check if the address of the sender of the transaction is in the participant addresses list
    bool found = false;
    for ( uint8 i = 0; i < participantAddresses.length && !found; i++ ) {
      found = participantAddresses[i] == msg.sender;
    }
    require( found, "Should be a participant." );
    _;
  }

  // constructor that can be given the name of the producer who is supposed to create the contract instance
  constructor( string name ) public {
    // mark the sender of the contract creation transaction as the energy producer
    producer = msg.sender;
    // add the producer as a participant
    addParticipant( name, ParticipantType.Producer );
  }

  // registers the sender as a consumer with the given name
  // can be called only if everybody have not yet registered
  // todo: should check that same participant does not register again
  function registerAsConsumer( string name ) public everybodyNotRegistered {
    // add as participant
    addParticipant( name, ParticipantType.Consumer );
    // if we have all allowed participants emit the EverybodyRegistered event, mark everybody registered and get the time
    if ( participantAddresses.length == PARTICIPANT_TOTAL ) {
      emit EverybodyRegistered();
      isEverybodyRegistered = true;
      everybodyRegisteredTime = now;
    }
  }

   // participant can report its energy usage / production for a period
   // amount should be energy in kilo wat hours multiplied by  the scale
  function reportEnergy( uint amount ) public everybodyRegistered isParticipant {
    // get the struct for the participant
    Participant storage participant = participants[ msg.sender ];
    // check that the participant has not already reported for this period
    require( !participant.reported, "Participant has already reported for this round." );
    if ( participant.participantType == ParticipantType.Consumer ) {
      // for consumer add its energy for the total energy usage for this period
      latestTotalUsed += amount;
      // and emit ConsumedEnergy event with the participants info.
      emit ConsumedEnergy( msg.sender, participant.name, amount );
    }

    else {
      // this is a producer so emit ProducedEnergy event
      emit ProducedEnergy( msg.sender, participant.name, amount );
    }

    // add the period's energy to participant's overall total
    participant.total += amount;
    // save period's energy
    participant.latest = amount;
    // mark participant as reported for this period
    participant.reported = true;

    // mark that one more participant has reported
    reportedNum++;
    if ( reportedNum == PARTICIPANT_TOTAL ) {
      // when everybody has reported for this period calculate how much solar energy each consumer has used
      calculateUsage();
    }
  }

   // calculates the amount of solar energy each consumer has used for the current period
   // energy is shared equally between consumers
  function calculateUsage() private {
    // mark reportedNum as 0 so that next period can begin after this
    reportedNum = 0;
    // get producer's info
    Participant storage prod = participants[ producer ];
    // the amount of energy produced for this period
    uint producerLatest = prod.latest;
    // go through each consumer and calcualte their share of the solar energy
    for ( uint8 i = 0; i < participantAddresses.length; i++ ) {
      // get participant's info
      address participantAddr = participantAddresses[i];
      Participant storage participant = participants[ participantAddr ];
      // mark as not reported so participant can report next period
      participant.reported = false;
      // get latest so it can be set as zero
      uint latest = participant.latest;
      participant.latest = 0;
      if ( producerLatest == 0 || latestTotalUsed == 0 ) {
        continue; // if no energy was produced or consumed we don't have to do anything more just prepare participants for next period
      }

      if ( participant.participantType == ParticipantType.Consumer ) {
        // calculate maximum amount of energy the participant could get from the producer
        // consumer's share depends on how much of the total used energy it used
        uint share = (producerLatest *latest) /latestTotalUsed;
        // how much consumer actually used of its share
        uint used = 0;
        if ( share <= latest ) {
          // used all of its share
          used = share;
        }

        else {
          // enough solar energy for all of the consumer's needs and some extra
          used = latest;
        }

        // add to consumer's total usage
        participant.localUsage += used;
        // add to producer's local usage
        prod.localUsage += used;
        // emit event about consumer's usage of solar energy for this period
        emit UsedSolar( participantAddr, participant.name, used, latest );
      }
    }

    // set to 0 for next period
    latestTotalUsed = 0;
    emit RoundCompleted();
  }

   // used internally to add a participant with given name and type
  function addParticipant( string name, ParticipantType pType ) private {
    // add struct representing participant to mapping
    participants[ msg.sender ] = Participant( name, pType, 0, 0, 0, false );
    // add address to addresses
    participantAddresses.push( msg.sender );
  }
}