# ProCem SolarUsage

This is an Ethereum smart contract based demo for keeping record of solar energy production and its use.
For a general overview of its purpose and architecture see section 5.3 of the
[ProCem project final report](http://www.senecc.fi/wp-content/uploads/2018/11/ProCem-loppuraportti.pdf)
 (in Finnish).
The basic structure of this project and some configuration files are based on the [webpack](https://truffleframework.com/boxes/webpack)
  Truffle box i.e. project template which is licensed under the MIT license.

## Requirements

To use this system you need Node.js and npm. You also need to have the [truffle](https://truffleframework.com/) and
[ganache-cli](https://github.com/trufflesuite/ganache-cli)
 npm modules installed globally:

    npm install -g truffle
    npm install -g ganache-cli

## Contents

The contents of this directory are explained shortly here.

- app: Code of SolarUsage monitor, the web based user interface for inspecting the SolarUsage contract state
- build: Directory created when the system is installed. Contains Truffle contract artifacts and SolarUsage monitor build outputs.
- contracts: Contains the SolarUsage contract source and also Truffle's default migrations contract that helps in managing contract deployments.
- migrations: Truffle's migration scripts  used in deploing contracts.
- tests: contains SolarUsage unit tests written in JavaScript.
- common_config.json: configuration file shared by all RTL Sender instances.
- energy.txt: sample energy measurements that can be used in testing the contract with file_send.js
- energyFileReader.js: module used by send_file.js to read the energy.txt file
- fake_rtl.js: a simple mock implementation of ProCem RTL data collector's get most recent measurement by id feature. Used in testing RTL Sender.
- fake.sh: a helper shell script for launching the system for testing with fake_rtl. Requires screen to work.
- file_send.sh: a helper shell script for launching the system for testing with send_file.js. Requires screen to work.
- getEvents_config.json: configuration file for getEvents.js
- getEvents.js: A command line tool for getting a specific number of SolarUsage events from ethereum.
- lift1.json: A instance specific configuration file for rtl_sender containing information about one participant.
- lift2.json: A instance specific configuration file for rtl_sender containing information about one participant.
- monitor_config.json: Configuration file for monitor.js.
- monitor.js: Command line based tool for monitoring SolarUsage contract participant information and events.
- package-lock.json: npm's packake-lock file
- package.json: npm's package.json
- present.js: command line utility for getting latest measurements by id from ProCem RTL data collector.
- rtl_sender.js: Application which gets energy measurements from the Procem RTL data collector and reports them to SolarUsage contract.
- send_file_config.json: configuration file of send_file.js
- send_file.js: A testing application that sends the contents of energy.txt to SolarUsage contract
- solarplant.json: A instance specific configuration file for rtl_sender containing information about one participant.
- truffle-config.js: Truffle configuration containin for example connection information about the Ethereum networks that can be used with truffle to for example deploy contracts.
- utils.js: Common functions for working with SolarUsage used by multiple applications such as SolarUsage monitor and monitor.js
- webpack.config.js: Configuration for webpack which is used to build the SolarUsage monitor web application.

## Installation

The system is installed with npm:

    npm install

This will:

- Install the required node.js packages.
- Compile the smart contracts (SolarUsage and the migration helper)
- Execute the SolarUsage contract unit tests.
- Build the SolarUsage monitor web application with webpack. Its code is also checked with eslint checker.

## Usage

All of the components that connect  to Ethereum requireconnection information.
This is given in their configuration files as web3-host and web3_port for the Ethereum RPC api via HTTP.
By default all components are configured to connect to 127.0.0.1:8545 i.e. localhost port 8545.
This is the default connection to ganache-cli the development Ethereum blockchain.
Ganache-cli is also configured as one of Truffle's networks with the name ganache_cli.

## SolarUsage

SolarUsage can be deployed to a Ethereum network with truffle. Before deployment the network has to be added to Truffle's configuration in truffle-config.js.
As stated previously ganache-cli is already configured. So after starting ganache-cli with:

    ganache-cli

You can deploy to it with:

    truffle migrate --network ganache_cli

You can execute the SolarUsage unit tests with (uses Truffle's own test blockchain no ganache-cli required):

    truffle test

## RTL Sender

For each participant of SolarUsage who gets their energy from ProCem data collector a RTL Sender instance should be lauched.
Each instance reads the following configuration information from common_config.json:

- rtl_host: ProCem data collector ip address
- rtl_port: data collector UDP port
- delay: Length of the reporting period in seconds.
- gasLimit: Ethereum gasLimit for transactions.
- gasPrice: Ethereum gasPrice for transactions.

When each instance is started it must be given a instance specific configuration file which has information about the participant it represents:

    node rtl_sender.js lift1.json

The configuration file should have the following:

- name: Name of the participant. For consumers this will be set as the participant name in the contract.
- id: The ProCem measurement id for the cumulative energy of the participant.
- producer: Boolean true if this is the energy producer false if this is the consumer.
- registered: boolean true if the participant is already registered to the contract. False if not in which case rtl sender first registers the participant. The producer is always already registered since the account who deployed the contract is marked as producer.
- address_index: The ethereum address of the participant from the accounts of the Ethereum client i.e. an index of web3.eth.accounts array.
- web3_host: Ethereum RPC api host for http connection.
- web3_port: Ethereum RPC api port for http connection.

Note  all RTL Sender instances should be launched at the same time so that they start reporting energies in the same rythm.
They get the first energy measurement at the start of next full period.
For example if period length is 60 seconds and RTL Sender is started at 12:00:40, it will get the first energy at 12:01:00.

## SolarUsage monitor

SolarUsage monitor is built with [webpack](https://webpack.js.org/).
Before building SolarUsage monitor's Ethereum connection has to be configured in app/scripts/config.json, if the default connection for ganache-cli is not suitable.
The build is done as part of the npm installation and can be done also with:

    npx webpack-cli --config ./webpack.config.js

For development it can be served for the browser with:

    npm run dev

This uses the webpack development server which automatically builds the application and refreshes the browser when a change is made.
Without these development features it can be served with:

    npm run server

This uses the [http-server](https://www.npmjs.com/package/http-server)
package: a simple command line based HTTP server. It serves the contents of the build directory in port 8080.

## Helper scripts

Two helpers scripts are provided for launching the system in two test scenarios.
Both launch a [screen](https://www.gnu.org/software/screen/)
session and start various components in different windows of that session.

### file_send.sh

This will:

- Launch ganache-cli with block time of 3 seconds
- Deploy SolarUsage to ganache-cli
- launch the command line based contract monitor (monitor.js)
- Start the send_file.js test program which sends example data from a file to the contract
- Connect to the screen session and open the window containing output from the monitor.

The block time is used to slow the process down to make it easier to observe since send_file reports the energies as fast as it can.

### fake.sh

This will:

- Launch ganache-cli
- Deploy SolarUsage to ganache-cli
- Start the SolarUsage monitor with the webpack development server
- Launch the fake_rtl.sh mock implementation of the data collector.
- Launch three instances of rtl_sender representing the solar plant and the two lifts
- Launch the command line based monitor
- Connect to the screen session and show the monitor window.