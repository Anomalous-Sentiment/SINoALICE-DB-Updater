# SINoALICE-DB-Updater
**NOTE: This project will no longer function, and is no longer being maintained due to SINoALICE Global ending service on 15 November 2023**

**IMORTANT NOTE: Using this can in fact cause your account to be banned (Although it happened quite late for me, like a month before EoS). Use a dummy account for safety**

A simple script for automatically updating SINoALICE Global data obtained from the game API. This forms the core of the SINoALICE Tracker project.

This repository also provides the necessary SQL scripts for setting up a PostgreSQL database for the script to store data into [here](https://github.com/Anomalous-Sentiment/SINoALICE-DB-Updater/tree/main/database). Data for populating the database **NOT** included.

## Overview
I realise that this repository may look a bit messy, so I'll add this section as a brief explanation of how this all works in case anyone wants to know.

The main Python script here is `DatabaseUpdater.py`. Upon starting the Docker container, `docker-entrypoint.sh` will be executed. This will essentially setup swap memory using up to 70% of the available disk space. This is done as all data pulled from the API will be stored in memory, and I do not want to risk running into an out of memory error, and I have no idea how much data will end up being stored in memory.

Once that is done, `start_updater.py` will be run. All this Python script does, is initialise an instance of `DatabaseUpdater`, and start it. From there, it will run a function to pull all player and guild data and populate the database. Then, once that is done, it will schedule that same update to run daily, 31 minutes after the server reset time.

### Daily Update
The daily update function can be summarised as the following tasks:
- Get all guilds and insert into database
- Get all players using the guild list, and insert into the database
- Check notices for any upcoming GC notices, and insert their dates into the database
    - If GC is available, schedule a function to pull GC ranking data after each GC timeslot
        - The GC update function also includes running the GC matchmaking function to predict the next GC match

### DatabaseGenerator.py
DatabaseGenerator.py is used to fill the database with dummy GC data. It is used only for testing purposes and requires the `guilds` table and `gc_events` table of the database to be filled first. Use it by calling the `regenerate_gc_data` function and provide the GC number to generate data for.


## Setup
This repository currently includes the following GitHub repository as a submodule
- [SINoALICE-Simplified-API](https://github.com/Anomalous-Sentiment/SINoALICE-Simplified-API)

Clone the required repositories by using the command:
```bash
git clone https://github.com/Anomalous-Sentiment/SINoALICE-DB-Updater.git --recurse-submodules
```

This is needed to pull the required SINoALICE-API submodule included in the repo. Note that this will pull from the latest commit of the main branch.

### SQL Database Setup
The [database](/database) directory contains all the SQL scripts needed to setup a PostgreSQL database for storing data.

Run them in the following order:
1. create_db.sql
2. create_views.sql
3. create_triggers.sql
4. init_data.sql

The remainder of the files are miscellaneous queries and not important. 

### Environment Variables
Refer to the `example.env` file to see what variables are required. All variables used by the [SINoALICE API project](https://github.com/Anomalous-Sentiment/SINoALICE-Simplified-API) are required, in addition to a database URL and logging URL. For reference, logging was implemented with Papertrail logging in mind.

## Using the Docker Image
For ease of use and simplicity, a Dockerfile has been provided to ensure minimal problems when running the Python scripts.

Ensure Docker is installed before continuing.

Use the following command to build the image:
```bash
docker build --tag sino-db-updater .
```

Use the following command to start the container:
```bash
docker run sino-db-updater
```