#!/bin/bash
# Copyright (c) TUT Tampere University of Technology 2015-2018.
# This software has been developed in Procem-project funded by Business Finland.
# This code is licensed under the MIT license.
# See the LICENSE.txt in the project root for the license terms.
#
# Main author(s): Ville Heikkila, Otto Hylli, Pekka Itavuo,
#                 Teemu Laukkarinen ja Ulla-Talvikki Virta
NL=`echo -ne '\015'`
screen -LdmS ethereum -t ethereum ganache-cli -b 3
sleep 4
truffle migrate --network ganache_cli
sleep 3
screen -S ethereum -X screen -t monitor
screen -S ethereum -p monitor -X stuff "node monitor.js$NL"
screen -S ethereum -X screen -t sender
screen -S ethereum -p sender -X stuff  "node send_file.js$NL"
screen -r ethereum -p monitor
