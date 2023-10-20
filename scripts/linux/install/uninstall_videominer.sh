#!/bin/bash

set -e
Color_Off='\033[0m'       # Text Reset
BGreen='\033[1;92m'       # Bold Green
BIRed='\033[1;91m'        # Red

error_handler()
{
  echo -e "${BIRed}Error: ($?) $1${Color_Off}"
  exit 1
}

while true; do
  read -p "Are you sure you would like to uninstall (Y/N)? " yn
  case $yn in
    [Yy]* ) break;;
    [Nn]* ) echo -e "${BGreen}Ok, keeping installation...no action taken."; exit 0;;
    * ) echo "Please answer yes or no.";;
  esac
done
echo "Uninstalling..."

systemctl stop videominer.service
sleep 2
systemctl disable videominer.service
rm -f /etc/systemd/system/videominer.service || error_handler "Unable to remove service.  Do you have enough permissions?\nTry running this command with sudo or as a user with sufficient privileges."
rm -f /etc/videominer_service/settings 
rm -f /usr/bin/videominer

echo -e "${BGreen}Video Miner service has been uninstalled."
echo -e "${Color_Off}"

exit 0
