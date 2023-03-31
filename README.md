# SINoALICE-DB-Updater
A simple script for automatically updating sinoalice data obtained from the API

## Cloning
Clone the repo by using the command:
`git clone https://github.com/Anomalous-Sentiment/SINoALICE-DB-Updater.git ----recurse-submodules` 

This is needed to include pull the required SINoALICE-API submodule included in the repo

## Using the Docker Image
Use the following command to build the image:
`docker build --tag sino-db-updater .`

Use the following command to start the container:
`docker run sino-db-updater`