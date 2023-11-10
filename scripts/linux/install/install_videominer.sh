#!/bin/bash

set -e

prompt1="Enter your ethereum address for use with Livepeer & Video Miner (starts with 0x): "
error_response_1="You must enter the Ethereum address for your transcoder node in order to be paid."
prompt2="Enter your authentication token to connect to the pool: "
error_response_2="You must enter the authentication token to connect to the pool."
prompt_sessions="Enter the maximum number of sessions your system can transcode (must be <= 10): "
error_response_sessions="You must enter the maximum number of sessions you wish to assign for transcoding.  The value must be a number less than or equal to 10."

SERVICE_SETTINGS_FILE="/etc/videominer_service/settings"
VM_TRANSCODER_ETH_ADDR=
VM_TRANSCODER_AUTH_TOKEN=
VM_NVIDIA_FLAGS="all"
RUN_SETUP=false
INSTALL_NEW_BIN=false
MAX_SESSIONS=10
Color_Off='\033[0m'       # Text Reset
BGreen='\033[1;32m'       # Bold Green
BIRed='\033[1;91m'        # Red

mkfile() 
{ 
  mkdir -p $( dirname "$1") && touch "$1" 
}

installbin() 
{ 
  downloadbin
  checkbin
  #move exec into /usr/bin and give execute perms
  cp videominer /usr/bin/
  chmod +x /usr/bin/videominer
  echo -e "Binary has been updated."
}

checkapp(){
  if ! command -v $1 > /dev/null 2>&1; then
    echo "The \"$1\" program is required by this script. Install \"$1\" and try again."
    exit 1
  fi
}

downloadbin(){

  OVERWRITE_WITH_LATEST=false
  FILE_EXISTS=0
  [ -f "videominer" ] || FILE_EXISTS=$?
  if ((FILE_EXISTS == 0)); then
    while true; do
      read -p "A binary named videominer was found in the current directory.  Would you like to replace this with the latest version (Y/N)? " yn
      case $yn in
        [Yy]* ) OVERWRITE_WITH_LATEST=true; break;;
        [Nn]* ) echo -e "Ok, using local binary.\n"; break;;
        * ) echo -e "Please answer yes or no.";;
      esac
    done
  fi
  
  if (((FILE_EXISTS == 1)) || $OVERWRITE_WITH_LATEST); then
    echo -e "Checking prerequisites..."
    checkapp "curl"
    checkapp "unzip"
    checkapp "jq"
    echo -e "Beginning download..."
    RELEASE_FILE_NAME="linux-videominer.tar.gz"
    ARTIFACTID=$(curl -s -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version:2022-11-28" https://api.github.com/repos/Video-Miner/Releases/releases/latest | \
      jq '.assets[] | select(.name | contains("linux")).id')
    dirname="videominer$(date '+%N')"
    DOWNLOAD_DIR=/tmp/$dirname
    mkdir $DOWNLOAD_DIR
    echo -e "${BGreen}Downloading latest package (artifact id = $ARTIFACTID) to ${DOWNLOAD_DIR}${Color_Off}"
    curl -fSL -H "Accept: application/octet-stream" -H "X-GitHub-Api-Version: 2022-11-28" -o "${DOWNLOAD_DIR}/${RELEASE_FILE_NAME}" "https://api.github.com/repos/Video-Miner/Releases/releases/assets/${ARTIFACTID}" 
    cd $DOWNLOAD_DIR
    tar -xf $RELEASE_FILE_NAME
    cd -
    cp $DOWNLOAD_DIR/videominer .
    rm -fdr $DOWNLOAD_DIR
    echo -e "${BGreen}Download complete.  The videominer binary is now in the current directory.${Color_Off}"
  fi
}

error_handler()
{
  echo -e "${BIRed}Error: ($?) $1${Color_Off}"
  exit 1
}

checkbin()
{
  #let's make sure we are running from the directory where the videominer bin is
  if [ ! -f ./videominer ]; then
    error_handler "You must run this script from the same directory that contains the videominer binary."
    exit 1
  fi
}

collect_gpus()
{
  # Get the GPU IDs as an array
  gpu_info=$(nvidia-smi --list-gpus)
  gpu_ids=($(echo "$gpu_info" | cut -d ' ' -f 2 | sed 's/:$//'))

  echo -e "\nHere are the GPUs found on your system:\n${BGreen}${gpu_info}${Color_Off}\n"

  read -p "Enter one or more GPU IDs you wish to use from the following list (${gpu_ids[*]}) separated by SPACES or the word 'all': " ids_entered

  # Split the input into an array
  IFS=' ' read -ra selected_ids <<< "$ids_entered"

  # Check if the input is valid
  valid_input=true
  selected_gpu_ids=()
  for id in "${selected_ids[@]}"; do
      if [[ "$id" == "all" ]]; then
          # If the input is 'all', add all GPU IDs to the selection
          selected_gpu_ids=("${gpu_ids[@]}")
      elif [[ "$id" =~ ^[0-9]+$ && " ${gpu_ids[@]} " =~ " $id " ]]; then
          # If the input is a single GPU ID, add it to the selection
          selected_gpu_ids+=("$id")
      else
          # If the input is invalid, print an error message and exit
          echo -e "${BIRed}Try again. Invalid input: $id${Color_Off}"
          valid_input=false
      fi
  done

  if $valid_input; then
      IFS=','
      VM_NVIDIA_FLAGS="${selected_gpu_ids[*]}"
  else
      collect_gpus
  fi
}

print_settings()
{
  echo -e "*********************************************"
  echo -e "Ethereum address \"$VM_TRANSCODER_ETH_ADDR\""
  echo -e "Authentication token \"$VM_TRANSCODER_AUTH_TOKEN\""
  echo -e "GPU IDs \"${VM_NVIDIA_FLAGS// /,}\""
  echo -e "Max Sessions \"$MAX_SESSIONS\""
  echo -e "*********************************************"
}

if test -f "$SERVICE_SETTINGS_FILE"; then
  echo -e "Found an existing service configuration...."
  
  #let's get the values and give a user the chance to change
  source "$SERVICE_SETTINGS_FILE"
	
  while true; do
    echo -e "Your system has already been configured with the below settings."
    print_settings
    read -p "Would you like to update these settings (Y/N)? " yn
    case $yn in
      [Yy]* ) RUN_SETUP=true; break;;
      [Nn]* ) echo -e "Ok, keeping existing configuration.\n"; break;;
      * ) echo -e "Please answer yes or no.";;
    esac
  done

else
  RUN_SETUP=true
fi

if $RUN_SETUP; then

  # system settings file not present or being reset, let's get user's eth addr and create it
  read -p "$prompt1" VM_TRANSCODER_ETH_ADDR
  read -p "$prompt2" VM_TRANSCODER_AUTH_TOKEN
  
  if [ "$VM_TRANSCODER_ETH_ADDR" = "" ]; then
    echo -e "${BIRed}$error_response_1${Color_Off}"
    exit 1
  fi
  if [ "$VM_TRANSCODER_AUTH_TOKEN" = "" ]; then
    echo -e "${BIRed}$error_response_2${Color_Off}"
    exit 1
  fi
	
  # get max sessions
  while true; do
    read -p "$prompt_sessions " session_input
    if [[ "$session_input" =~ ^[0-9]+$ ]] && [ "$session_input" -lt 11 ]; then
      MAX_SESSIONS="$session_input"; break;
    else
    echo -e "${BIRed}$error_response_sessions${Color_Off}"
    fi
  done

  collect_gpus

  mkfile $SERVICE_SETTINGS_FILE || error_handler "Unable to save your settings during install.  Do you have enough permissions?\nTry running this command with sudo or as a user with sufficient privileges to install services."
  printf "VM_TRANSCODER_ETH_ADDR=%s\nVM_TRANSCODER_AUTH_TOKEN=%s\nVM_NVIDIA_FLAGS=%s\nMAX_SESSIONS=%s" "$VM_TRANSCODER_ETH_ADDR" "$VM_TRANSCODER_AUTH_TOKEN" "${VM_NVIDIA_FLAGS// /,}" "$MAX_SESSIONS" > "$SERVICE_SETTINGS_FILE"
  print_settings
  echo -e "${BGreen}Your Ethereum address for Video Miner has been configured with the above settings and saved in \"$SERVICE_SETTINGS_FILE\"."
  echo -e "${Color_Off}This must be a valid Ethereum address or the service will fail.  You can reset this by re-running this script if necessary."
fi

# Install the service file if it doesn't exist
if [ ! -f /etc/systemd/system/videominer.service ]; then
  echo -e "Preparing Video Miner service..."
  installbin
  cat > /etc/systemd/system/videominer.service << EOF
[Unit]
Description=Video Miner
After=network.target
After=network-online.target

[Service]
Type=simple
EnvironmentFile=$SERVICE_SETTINGS_FILE
ExecStart=videominer -ethAcctAddr \${VM_TRANSCODER_ETH_ADDR} -orchSecret \${VM_TRANSCODER_AUTH_TOKEN} -nvidia \${VM_NVIDIA_FLAGS// /,} -maxSessions \${MAX_SESSIONS} -dataDir %h/.videominer -v 6
Restart=on-failure
KillSignal=SIGINT

[Install]
WantedBy=multi-user.target
EOF

  #enable and start the service
  echo -e "Reloading daemon and enabling service"
  systemctl daemon-reload
  systemctl enable videominer.service
  systemctl start videominer.service
  
else

  while true; do
    read -p "Would you like to upgrade the Video Miner application (Y/N)? " yn
    case $yn in
      [Yy]* ) INSTALL_NEW_BIN=true; break;;
      [Nn]* ) echo -e "Ok, keeping existing binary.\n"; break;;
      * ) echo -e "Please answer yes or no.";;
    esac
  done

  if $INSTALL_NEW_BIN; then
    echo -e "Stopping service and replacing binary..."
    systemctl stop videominer.service
    installbin 
  fi

fi



if $RUN_SETUP || $INSTALL_NEW_BIN; then
  echo -e "Restarting service..."
  systemctl restart videominer.service
  systemctl status videominer.service

  echo -e "${BGreen}The Video Miner service has been configured and started.  It will automatically start when your system starts up."
  echo -e "The last 35 lines of the log file can be view with this command: journalctl -u videominer -n 35 --no-pager"
  echo -e "The service can be managed with: systemctl (start|stop|restart|status) videominer"
else
  echo -e "${BGreen}No changes made."
fi

echo -e "${Color_Off}"
exit 0
