#!/bin/bash
# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta
screen -LdmS solarusage -t ethereum ganache-cli
sleep 4
truffle migrate --network ganache_cli
sleep 2
screen -S solarusage -X screen -t http npm run dev
screen -S solarusage -X screen -t rtl node fake_rtl.js
screen -S solarusage -X screen -t monitor node monitor.js
screen -S solarusage -X screen -t lift1 node rtl_sender.js lift1.json
screen -S solarusage -X screen -t lift2 node rtl_sender.js lift2.json
screen -S solarusage -X screen -t solarplant node rtl_sender.js solarplant.json
screen -r solarusage -p monitor
