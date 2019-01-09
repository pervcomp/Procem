#!/bin/sh
screen -d -m -L -S procem_rtl -t restarter python3 -u process_starter.py procem_components.json
screen -S procem_rtl -X screen -t rtl python3 -u procem_rtl.py rtl_configuration.json
screen -S procem_rtl -X screen -t backup python3 -u backup_procem_data.py backup_procem_data.json
screen -S procem_rtl -X screen -t udp python3 -u udp_send_checker.py udp_send_checker.json
cd adapters
screen -d -m -L -S adapters -t weatherstation python3 -u postgres_generic_adapter.py postgres_weatherstation_config.json Wheather_Station_measurement_IDs.csv
screen -S adapters -X screen -t iss python3 -u modbus_generic_adapter.py modbus_ISS_config.json ISS_measurement_IDs.csv
screen -S adapters -X screen -t solarplant python3 -u modbus_generic_adapter.py modbus_solarplant_config.json Solar_Plant_measurement_IDs.csv
screen -S adapters -X screen -t bacnet python3 -u bacnet_adapter.py bacnet_config.json
screen -S adapters -X screen -t laatuvahti python3 -u mxelectrix_adapter.py mxelectrix_main_config.json
screen -S adapters -X screen -t nordpool python3 -u rest_generic_adapter.py nordpool_configuration.json
screen -S adapters -X screen -t fingrid python3 -u rest_generic_adapter.py fingrid_configuration.json
screen -S adapters -X screen -t hirlam python3 -u rest_generic_adapter.py hirlam_configuration.json
