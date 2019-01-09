/* SolarUsage Monitor main Javascript file.
 * Shows events and participant information on the web page.
 */
// Original work Copyright (c) Truffle 2018.
// Modified work Copyright (c) TUT Tampere University of Technology 2015-2018.
// This software has been developed in Procem-project funded by Business Finland.
// This code is licensed under the MIT license.
// See the LICENSE.txt in the project root for the license terms.
//
// Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
//                 Teemu Laukkarinen ja Ulla-Talvikki Virta

// Import the page's CSS. Webpack will know what to do with it.
import '../styles/app.css'

// Import libraries we need.
import { default as Web3 } from 'web3'
import { default as contract } from 'truffle-contract'
import $ from 'jquery'
import utils from '../../utils.js'

// Import our contract artifacts and turn them into usable abstractions.
import solarUsageArtifact from '../../build/contracts/SolarUsage.json'
const SolarUsage = contract(solarUsageArtifact)
// import config which has ethereum connection configuration
import config from './config.json'

// App will contain all functionality of SolarUsage monitor
const App = {
  // code that initializes the application: creates SolarUsage instance, sets up event listeners
  // and gets initial data if available
  start: async function () {
    const self = this
    // mark that we do not yet have the participants table on the page
    self.tableCreated = false
    // set web3 provider to contract and create instance
    SolarUsage.setProvider(web3.currentProvider)
    self.instance = await SolarUsage.deployed()
    // show that we are connected to ethereum and the contract on the page
    self.setStatus('Connected to contract.')
    // get the conversion rate between the energy in the contract and kilowatt hours
    let scale = await self.instance.SCALE.call()
    // give some values to utils  which needs them to work properly
    utils.scale = scale
    utils.web3 = web3
    utils.instance = self.instance
    // the precision we show numbers from the contract events
    utils.eventPrecision = 4

    // get name of producer which is  used when updating participants table
    let producerAddr = await self.instance.producer.call()
    self.producerName = utils.buildParticipant(await self.instance.participants.call(producerAddr)).name

    // get the element we will add events later
    self.eventList = $('#events')
    // initialize how many events we have now and which is the maxinum number of events we will show
    self.eventCount = 0
    self.eventAmount = 6

    // setup event listeners for the different events
    // all events will get showEvents used to add them to the events list
    self.instance.EverybodyRegistered().watch(self.showEvent)

    // status message to be shown when everybody is registered
    let registeredStatus = 'Everybody registered. Monitoring contract events and participant energy totals.'

    // when everybody have registered we will change the status message shown on the page
    self.instance.EverybodyRegistered().watch(() => self.setStatus(registeredStatus))
    // also we will add the time of EverybodyRegistered above the participants table
    self.instance.EverybodyRegistered().watch(self.updateRegistered)
    self.instance.ProducedEnergy().watch(self.showEvent)
    self.instance.ConsumedEnergy().watch(self.showEvent)
    self.instance.UsedSolar().watch(self.showEvent)
    self.instance.RoundCompleted().watch(self.showEvent)
    // when round is completed we will update the participants table
    self.instance.RoundCompleted().watch(self.updateParticipants)

    if (await self.instance.isEverybodyRegistered.call()) {
      // everybody have already registered before we started
      // lets update status and get initial information: participants' energies, newest events
      // and the everybody registered time
      self.setStatus(registeredStatus)
      self.getWhenRegistered()
      self.updateParticipants()
      self.getOldEvents(self.eventAmount)
    } else {
      // everybody have not yet registered so we just update status and start waiting for things to happen
      self.setStatus('Waiting for participants to register to contract.')
    }
  },

  // function used to update the status message on the page
  setStatus: function (message) {
    $('#status').html(message)
  },

  // gets the time when everybody had registered and shows it on the page
  // this time tells from which moment forward the contract has data
  getWhenRegistered: async function () {
    // const self = this probably did not work here since method is called from other App's method or something
    const self = App
    let time = await utils.getWhenRegistered()
    self.setRegisteredTime(time)
  },

  // handler for EverybodyRegistered which updates the event's time to the page above the participants table
  updateRegistered: function (err, event) {
    const self = App
    if (err) {
      return console.log(err)
    }

    // get the time of the event since the event object just contains the block number and
    // we have to get the corresponding block's timestamp
    utils.addTimeToEvent(event)
    self.setRegisteredTime(event.time)
  },

  // show the given Date as everybody registered on time above participants table
  setRegisteredTime: function (time) {
    $('#registered').html(' on ' + time.toString().split(' GMT')[0])
  },

  // SolarUsage event handler that shows the event in the events list
  showEvent: function (err, event) {
    const self = App
    if (err) {
      console.log(err)
      return
    }

    // we want to show event time but the objects we get do not contain it
    // they have only a block number so we have to get the timestamp of the corresponding block
    utils.addTimeToEvent(event)
    // format a string from the event's information
    let message = utils.eventToString(event)
    // add the event to the beginning of the list
    self.eventList.prepend('<li role="status">' + message + '</li>')

    // we keep count of events we have and if we have more than the max amount we want to show we will remove event
    // from the end of the list
    self.eventCount += 1
    if (self.eventCount > self.eventAmount) {
      $('li:last-child', self.eventList).remove()
    }
  },

  // function that gets the given amount of latest events and adds them to the events list
  getOldEvents: function (amount) {
    const self = this
    utils.getOldEvents(amount, 'allEvents', (err, result) => {
      if (err) {
        return console.log(err)
      }

      // have to reverse the list since it has newest first but showEvent adds to the beginning of the list
      result.reverse().forEach(event => self.showEvent(null, event))
    })
  },

  // updates information about energy amounts in the participants table
  updateParticipants: async function () {
    const self = App
    // get all Participant structs from the contract
    let data = await utils.getAllParticipants()
    if (!self.tableCreated) {
      // first time we get the data
      // we have to first create the table before adding data to it
      console.log('Creating participants table.')
      self.tableCreated = true
      self.createTable(data)
    }

    console.log('Updating participants table.')
    // lets go through each table column and update its data from each participant
    self.participantView.properties.forEach(property => {
      data.forEach(participant => {
        // except we don't want to show producers localUsage
        if (!(participant.name === self.producerName && property === 'localUsage')) {
          // we will not show numbers in their full precision
          self.participantView[participant.name][property].html(utils.round(participant[property], 2))
        }
      })
    })
  },

  // creates the table containing participants' energies
  // participants' information converted in to an object with utils is the parameter
  createTable: function (data) {
    const self = this
    // we will add the table cells that will later be updated with energy amounts here
    let participantView = {}
    self.participantView = participantView
    let table = $('#participants')
    // construct first row consisting of participant names
    let heading = '<tr><td>Name</td>'
    data.forEach(item => {
      heading += '<td class="data">' + item.name + '</td>'
      // place for participant's energies
      participantView[item.name] = {}
    })

    heading += '</tr>'
    table.append(heading)

    // participant properties we will show in the table
    // each of these gets their own row
    participantView.properties = [ 'total', 'localUsage' ]
    // map the property names to row names shown on the page
    let rowNames = {
      'total': 'Total energy',
      'localUsage': 'Used solar energy'
    }

    // lets add the rows including elements where the energies will be updated later
    participantView.properties.forEach(property => {
      let row = $('<tr><td>' + rowNames[property] + '</td></tr>')
      // add element for each participants' data for this row
      data.forEach(participant => {
        let td = $('<td class="data"></td>')
        row.append(td)
        // through this we can access the element easily when we want to update its content
        participantView[participant.name][property] = td
      })

      table.append(row)
    })
  }
}

// make app global so it can be accessed from console
window.App = App

// Start the monitor after page has loaded
window.addEventListener('load', async function () {
  // make jquery global (can be used from browser's JavaScript console
  window.$ = $
  // Checking if Web3 has been injected by the browser (Mist/MetaMask)
  if (typeof web3 !== 'undefined') {
    console.warn('Using web3 detected from external source.')
    // Use Mist/MetaMask's provider
    window.web3 = new Web3(web3.currentProvider)
  } else {
    console.warn('No web3 detected. Creating one based on configuration.')
    let url = 'http://' + config.web3_host + ':' + config.web3_port
    window.web3 = new Web3(new Web3.providers.HttpProvider(url))
  }

  App.start()
})
