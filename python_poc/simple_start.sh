#!/bin/sh
screen -d -m -L -S procem_rtl -t restarter python3 -u process_starter.py procem_components.json
