from sinoalice.api.GuildAPI import GuildAPI
from sinoalice.api.PlayerAPI import PlayerAPI
from sinoalice.api.GranColoAPI import GranColoAPI
from sinoalice.api.NoticesParser import NoticesParser
from sqlalchemy import create_engine, Table, MetaData, select, desc, asc, join, func, and_
from sqlalchemy.dialects.postgresql import insert
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, date, timedelta, time
from apscheduler.schedulers.background import BlockingScheduler
import os
from pytz import utc
import itertools
import json
import time as other_time
import logging
import socket
from logging.handlers import SysLogHandler
import traceback
import math
load_dotenv()


class ContextFilter(logging.Filter):
    hostname = socket.gethostname()
    def filter(self, record):
        record.hostname = ContextFilter.hostname
        return True


syslog = SysLogHandler(address=((os.getenv('LOGGING_URL'), int((os.getenv('LOGGING_PORT'))))))
syslog.addFilter(ContextFilter())

logging.Formatter.converter = other_time.gmtime
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
syslog.setFormatter(formatter)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# Add the handler we created
log.addHandler(syslog)
# Only used because syslog doesn't handle multline excpetions with indentation
def log_exception(msg, tb):
    # Log the message
    log.error(msg)

    # Log every line of exception separately to maintain indentation
    lines = tb.split('\n')
    for l in lines:
        # Check line exceeds 1024 bytes (UDP) OR 100,000 bytes (TCP)
        # UTF 8 max char size = 4 bytes
        # ASCII max char size = 1 byte
        # UTF 8 max chars = 25,000
        # However, I don't expect such long messages to be of any use, so using a smaller limit
        # Note: This is prone to sending logs out of order due to the nature of networks...
        max_line_length = 15000
        if len(l) <= max_line_length:
            log.error(l)
        else:
            # Print max number of characters followed by truncated message
            log.error(l[:max_line_length] + ' --Line Truncated--')

class DatabaseUpdater():
    def __init__(self):
        self.job_list = []
        self.sched = BlockingScheduler(timezone=utc)
        # Pre pool ping because database might not be started yet.
        self.engine = create_engine(os.getenv('POSTGRES_URL'), pool_pre_ping=True, connect_args={'sslmode':'require'})
        self.metadata = MetaData()
        self.guild_table = Table(
            'guilds', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.player_data_table = Table(
            'base_player_data', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.extra_player_data_table = Table(
            'extra_player_data', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.gc_data_table = Table(
            'gc_data', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.day_0_table = Table(
            'temp', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.day_table = Table(
            'gc_days', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.timeslot_table = Table(
            'timeslots', 
            self.metadata, 
            autoload_with=self.engine
        )
        self.match_table = Table(
            'gc_predictions', 
            self.metadata, 
            autoload_with=self.engine
        )

        # This is a view
        self.formatted_match_table = Table(
            'gc_matchups_id', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.gc_event_table = Table(
            'gc_events', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.gc_finals_table = Table(
            'gc_finals', 
            self.metadata, 
            autoload_with=self.engine
        )

        log.info('DatabaseUpdater Initialised')

    def run(self):
        # Run daily update once on start
        self._daily_update()

        # Shedule to run the update task every day, 31 min after reset
        self.sched.add_job(self._daily_update, 'cron', hour=5, minute=31)
        log.info('DatabaseUpdater starting...')
        self.sched.start()

    def _update_guilds_table(self, guild_list):
        log.info('Guild table update starting...')
        converted_list = []

        # Get the list of table columns, excluding guilddataid and updated_at
        table_cols = self.metadata.tables['guilds']
        full_guild_columns = [column.key for column in table_cols.columns]
        guild_columns =  list(filter(lambda col: col != 'updated_at', full_guild_columns))

        # Process guild dict keys to suitable format
        for guild in guild_list:
            if 'createdTime' in guild:
                guild['createdTime'] = datetime.utcfromtimestamp(int(guild['createdTime'])).isoformat()

            # Convert to lower case keys
            guild =  {k.lower(): v for k, v in guild.items()}

            # Set value to null if column not in dict
            for column in guild_columns:
                if column not in guild:
                    guild[column] = None

            # Delete extra keys to avoid issues with insert
            for key in list(guild.keys()):
                if key not in guild_columns:
                    #print('Extra key: ' + key)
                    # Extra key/column in data, remove
                    del guild[key]

            converted_list.append(guild)

        # Create insert and update statements
        insert_stmt = insert(self.guild_table).values(converted_list)
        # Update all column names except primary key
        update_columns = {col.name: col for col in insert_stmt.excluded if col.name not in ('guilddataid')}
        update_statement = insert_stmt.on_conflict_do_update(
            index_elements=['guilddataid'], 
            set_=update_columns
        )

        # Execute command
        with self.engine.connect() as conn:
            conn.execute(update_statement)
            conn.commit()

        log.info('Guild table update complete')

    def _update_guild_gm_data(self, guild_list):
        # Function for updating the GM data of the guild list passed in (For when we do not need to update the entire player table. This is for maintaining referential integrity)
        gm_id_list = []
        for guild in guild_list:
            # Add ever guild gm id to list
            gm_id_list.append(guild['guildMasterUserId'])

        log.info(f'GM ID list length: {len(gm_id_list)}')

        # Get the data
        player_api = PlayerAPI()
        gm_data_list = player_api.get_selected_player_data(gm_id_list)
        log.info(f'Length of gm player data:{len(gm_data_list)}')

        # Process the data
        gm_data_list = self._process_base_player_data(gm_data_list)
        log.info(f'Length of processed data list:{len(gm_data_list)}')

        # Insert into db
        self._insert_base_player_data_db(gm_data_list)


    def _process_base_player_data(self, base_player_list):
        log.info('Processing player data...')
        converted_player_list = []
        base_table_cols = self.metadata.tables['base_player_data']
        base_player_data_columns = [column.key for column in base_table_cols.columns]
        base_player_data_columns =  list(filter(lambda col: col != 'updated_at', base_player_data_columns))

        converted_base_player_list = []
        for base_player in base_player_list:
            player =  {k.lower(): v for k, v in base_player.items()}

            # Add checks like extra player data & guild data to remove extra keys, or set keys to none if not existing
            for column in base_player_data_columns:
                if column not in player:
                    #print('Key not in data: ' + column)
                    player[column] = None

            for key in list(player.keys()):
                if key not in base_player_data_columns:
                    #print('Extra key: ' + key)
                    # Extra key/column in data, remove
                    del player[key]

            converted_base_player_list.append(player)

        return converted_base_player_list

    def _process_extra_player_data(self, player_list):
        log.info('Processing extra player data...')
        converted_player_list = []
        table_cols = self.metadata.tables['extra_player_data']
        extra_player_data_columns = [column.key for column in table_cols.columns]
        extra_player_data_columns =  list(filter(lambda col: col != 'updated_at', extra_player_data_columns))


        # Convert data for insertion into db
        for player in player_list:
            if 'cleaningUpdatedTime' in player['userData'] and player['userData']['cleaningUpdatedTime'] != None:
                player['userData']['cleaningUpdatedTime'] = datetime.utcfromtimestamp(int(player['userData']['cleaningUpdatedTime'])).isoformat()
            if 'createdTime' in player['userData'] and player['userData']['createdTime'] != None:
                player['userData']['createdTime'] = datetime.utcfromtimestamp(int(player['userData']['createdTime'])).isoformat()
            if 'lastAccessTime' in player['userData'] and player['userData']['lastAccessTime'] != None:
                player['userData']['lastAccessTime'] = datetime.utcfromtimestamp(int(player['userData']['lastAccessTime'])).isoformat()
            if 'staminaUpdatedTime' in player['userData'] and player['userData']['staminaUpdatedTime'] != None:
                player['userData']['staminaUpdatedTime'] = datetime.utcfromtimestamp(int(player['userData']['staminaUpdatedTime'])).isoformat()
            if 'tutorialFinishTime' in player['userData'] and player['userData']['tutorialFinishTime'] != None:
                player['userData']['tutorialFinishTime'] = datetime.utcfromtimestamp(int(player['userData']['tutorialFinishTime'])).isoformat()

            player =  {k.lower(): v for k, v in player['userData'].items()}

            for column in extra_player_data_columns:
                if column not in player:
                    #print('Key not in data: ' + column)
                    player[column] = None

            for key in list(player.keys()):
                if key not in extra_player_data_columns:
                    #print('Extra key: ' + key)
                    # Extra key/column in data, remove
                    del player[key]

            converted_player_list.append(player)
        return converted_player_list

    def _update_players(self, guild_list):
        log.info('Player table update starting...')
        converted_player_list = []
        player_api = PlayerAPI()

        # Get the players in guilds
        log.info('Getting guild player data (extra data)...')
        player_list = player_api.get_players_in_guilds(guild_list=guild_list)
        log.info('Guild player data retrieval successful')

        # Get player ids from db
        db_player_id_list = self._get_player_ids_from_db()

        player_id_set = set(db_player_id_list)

        # Add player ids from api to ids from db if unique using unique properties of set
        for player_data in player_list:
            player_id_set.add(player_data['userData']['userId'])

        # Get the list of basic player info using player ids
        log.info('Getting player profile data (Base player data)...')
        base_player_list = player_api.get_basic_player_info(list(player_id_set))
        log.info('Player profile data retrieval successful')

        # Convert data into form suitable for database
        converted_base_player_list = self._process_base_player_data(base_player_list)


        # convert extra player data into form suitable for db
        converted_player_list = self._process_extra_player_data(player_list)

        self._insert_base_player_data_db(converted_base_player_list)
        self._insert_extra_player_data(converted_player_list)

        log.info('Player table update complete')


    def _insert_base_player_data_db(self, converted_base_player_list):
        with self.engine.connect() as conn:
            log.info('Inserting base player data into DB...')
            batch_size = 5000
            total_batches = math.ceil(len(converted_base_player_list) / batch_size)

            # Break data into chunks to avoid timeout when doing all at once
            for batch_num, batch in enumerate(self._chunks(converted_base_player_list, batch_size)):
                log.info(f'Inserting batch {batch_num + 1} of {total_batches}')
                insert_base_player_stmt = insert(self.player_data_table).values(batch)
                update_columns = {col.name: col for col in insert_base_player_stmt.excluded if col.name not in ('userid')}
                update_statement = insert_base_player_stmt.on_conflict_do_update(
                    index_elements=['userid'], 
                    set_=update_columns
                )
                conn.execute(update_statement)
                conn.commit()
            log.info('Insert Successful')


    def _insert_extra_player_data(self, converted_player_list):
        with self.engine.connect() as conn:
            batch_size = 5000

            log.info('Inserting extra player data into DB...')
            total_batches = math.ceil(len(converted_player_list) / batch_size)

            # Break data into chunks to avoid timeout when doing all at once
            for batch_num, batch in enumerate(self._chunks(converted_player_list, batch_size)):
                log.info(f'Inserting batch {batch_num + 1} of {total_batches}')
                insert_stmt2 = insert(self.extra_player_data_table).values(batch)
                update_columns2 = {col.name: col for col in insert_stmt2.excluded if col.name not in ('userid')}
                update_statement2 = insert_stmt2.on_conflict_do_update(
                    index_elements=['userid'], 
                    set_=update_columns2
                )
                conn.execute(update_statement2)
                conn.commit()

            log.info('Insert Successful')

     # From stack overflow. 
    def _chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n] 

    def _insert_gc_data_db(self, gc_data, day):
        converted_data = []
        table_cols = self.metadata.tables['gc_data']
        gc_data_columns = [column.key for column in table_cols.columns]
        gc_data_columns =  list(filter(lambda col: col != 'updated_at', gc_data_columns))


        log.info('Converting GC data...')
        for data in gc_data:
            new_data =  {k.lower(): v for k, v in data.items()}
            new_data['gcday'] = day

            # If column not in data, then add it and set it to null
            for column in gc_data_columns:
                if column not in new_data:
                    #print('Key not in data: ' + column)
                    new_data[column] = None

            # Delete key if not in columns
            for key in list(new_data.keys()):
                if key not in gc_data_columns:
                    #print('Extra key: ' + key)
                    # Extra key/column in data, remove
                    del new_data[key]

            converted_data.append(new_data)
        log.info('Conversion complete')

        # Insert into database 
        insert_gc_ranks = insert(self.gc_data_table).values(converted_data)
        update_gc_ranks = {col.name: col for col in insert_gc_ranks.excluded if col.name not in ('gcday', 'gvgeventid', 'guilddataid')}
        update_statement = insert_gc_ranks.on_conflict_do_update(
            index_elements=['gcday', 'gvgeventid', 'guilddataid'], 
            set_=update_gc_ranks
        )

        log.info('Inserting GC data...')
        with self.engine.connect() as conn:
            conn.execute(update_statement)
            conn.commit()
        log.info('Inserting successful')

    def _full_gc_rank_update(self, day=None):
        log.info('Full GC rank update starting...')
        gc_api = GranColoAPI()
        guild_api = GuildAPI()

        # Get the rank list of the time slot
        log.info('Retrieving full GC rank list using API...')
        full_rank_list = gc_api.get_full_rank_list() 
        log.info(f'Retrieval successful, rank list length:{len(full_rank_list)}')

        # Update the guilds table to add all guilds participating in GC, in case they are not in th DB already
        log.info('Getting guild list...')
        gc_guild_list = guild_api.get_selected_guilds(full_rank_list)
        log.info('Guild list retrieved using API successfully')

        log.info('Updating GM player data of guilds participating in GC...')
        # Update the player data of guilds participating in GC
        # Needed or we risk having a guild master who does not exist in the base player table (Foreign key error)
        self._update_guild_gm_data(gc_guild_list)
        log.info('GM data update complete')



        log.info('Inserting participating guilds into DB...')
        self._update_guilds_table(gc_guild_list)
        log.info('Guild Insert complete')

        self._insert_gc_data_db(full_rank_list, day)
#
        log.info('Full GC rank update complete')

    def _daily_update(self):
        try:
            log.info('Starting daily update...')

            notices_parser = NoticesParser()

            # TODO: Implement check to see if maintainence is in progress to avoid errors

            # Update the guilds
            guild_api = GuildAPI()

            # Update the players using guild list (Get from database)
            guild_id_list = self._get_guild_ids()
            guild_list = guild_api.get_guild_list(guild_id_list)

            # Update players with revised guild id list
            self._update_players(guild_list)
            # Once players updated, update guilds
            self._update_guilds_table(guild_list)

            # Check if GC dates available
            gc_dates = notices_parser.get_gc_dates()

            if gc_dates != None:
                log.info('GC dates found: ' + str(gc_dates))
                # Run the function to schedule gc ranking updates
                self._schedule_gc_updates(gc_dates)

            log.info('Daily update complete')
        except:
            tb = traceback.format_exc()
            log_exception('Daily update failed.', tb)

    def _get_gc_timeslots(self):
        # Get timeslots where gc is held
        ts_stmt = select(self.timeslot_table.c.timeslot, self.timeslot_table.c.time_in_utc).where(self.timeslot_table.c.gc_available == True)

        with self.engine.connect() as conn:
            ts_data = conn.execute(ts_stmt).all()
            conn.commit()

        # Format into list of dicts?

        return ts_data

    def _update_db_gc_dates(self, gc_num, gc_date_dict):
        log.info('Updating GC dates...')
        data = [
            {
                'gvgeventid': gc_num,
                'entry_start': gc_date_dict['entry']['start'].isoformat(),
                'entry_end': gc_date_dict['entry']['end'].isoformat(),
                'prelim_start': gc_date_dict['prelims']['start'].isoformat(),
                'prelim_end': gc_date_dict['prelims']['end'].isoformat()
            }
        ]

        finals_data = [
            {
                'gvgeventid': gc_num,
                'finals_group': 'A',
                'start_time': gc_date_dict['finals']['grp_a_start'].isoformat(),
                'end_time': gc_date_dict['finals']['grp_a_end'].isoformat()
            },
            {
                'gvgeventid': gc_num,
                'finals_group': 'B',
                'start_time': gc_date_dict['finals']['grp_b_start'].isoformat(),
                'end_time': gc_date_dict['finals']['grp_b_end'].isoformat()
            }
        ]

        # Update db with new GC dates
        gc_date_stmt = insert(self.gc_event_table).values(data)
        update_gc_dates = {col.name: col for col in gc_date_stmt.excluded if col.name not in ('gvgeventid')}
        update_statement = gc_date_stmt.on_conflict_do_update(
            index_elements=['gvgeventid'], 
            set_=update_gc_dates
        )

        gc_finals_data_stmt = insert(self.gc_finals_table).values(finals_data)
        update_gc_finals = {col.name: col for col in gc_finals_data_stmt.excluded if col.name not in ('gvgeventid')}
        update_finals_statement = gc_finals_data_stmt.on_conflict_do_update(
            index_elements=['gvgeventid', 'finals_group'], 
            set_=update_gc_finals
        )

        with self.engine.connect() as conn:
            log.info('Inserting GC prelim dates into DB...')
            conn.execute(update_statement)
            log.info('Insert successful')
            conn.commit()
            log.info('Inserting GC finals dates into DB...')
            conn.execute(update_finals_statement)
            conn.commit()
            log.info('Insert successful')


    def _get_guild_ids(self):
        converted_list = []
        # Select all guilddataid s from table
        guild_statement = select(self.guild_table.c.guilddataid)

        with self.engine.connect() as conn:
            guild_id_list = conn.execute(guild_statement).all()
            conn.commit()

        # Convert to list of dicts
        for guilddataid, in guild_id_list:
            converted_row = {
                'guildDataId': guilddataid
            }
            converted_list.append(converted_row)

        return converted_list

    def _get_player_ids_from_db(self):
        id_list = []
        # Select all guilddataid s from table
        player_statement = select(self.player_data_table.c.userid)

        with self.engine.connect() as conn:
            player_id_list = conn.execute(player_statement).all()
            conn.commit()

        # Convert to list of dicts
        for userid, in player_id_list:
            id_list.append(userid)

        return id_list

    def _schedule_gc_updates(self, date_dict):
        try:    
            log.info('Scheduling GC updates...')
            start_date = date_dict['prelims']['start']
            end_date = date_dict['prelims']['end']

            # Use the first month of gc to calculate the current GC number
            # This assumes that a GC occurs every month, and only once every month. This will become inaccurate otherwise
            first_gc_month = datetime(2020, 8, 1)
            curr_gc = ((start_date.year - first_gc_month.year) * 12 + start_date.month - first_gc_month.month) + 1

            log.info('Current GC: ' + str(curr_gc))

            # Calculate number of days in prelims (In case it ever changes, though I doubt it will)
            # The 1 second timedelta is because the end datetime is 1 sec short of being a full 6 days right now
            prelim_days = (end_date - start_date + timedelta(seconds=1)).days

            # Update db with GC dates
            self._update_db_gc_dates(curr_gc, date_dict)

            # Remove all previously scheduled gc updates
            for job in self.job_list:
                job.remove()

            # Schedule the initial gc rank update on day 2, 1 min after reset
            day_2_job = self.sched.add_job(self._day_2_update, run_date=(start_date + timedelta(days=1, minutes=3)), args=[curr_gc], id=f'gc_{curr_gc}_day_2_update')
            self.job_list.append(day_2_job)

            gc_timeslots = self._get_gc_timeslots()

            # Schedule an update of the gc ranks after every time slot's colo starting from day 2 (0 indexed, idx 1 = day 2)
            for day in range(1, prelim_days):
                for timeslot, utc_time in gc_timeslots:
                    # Calculate the time to run the update

                    # Check if time earlier than reset time (5 AM UTC)
                    if utc_time < time(hour=5, minute=00, second=00, tzinfo=utc):
                        # If earlier, increase the day by 1 (Because GC only starts after reset)
                        update_datetime = start_date + timedelta(days=day + 1)
                        update_datetime = update_datetime.replace(hour=utc_time.hour, minute=utc_time.minute + 33, second=utc_time.second)
                    else:                            
                        # Else, do not modify day value
                        update_datetime = start_date + timedelta(days=day)
                        # Set the hours and minute where the colo ends + 33 min
                        update_datetime = update_datetime.replace(hour=utc_time.hour, minute=utc_time.minute + 33, second=utc_time.second)

                    # Check if current day is before final day
                    if day < prelim_days:
                        predict = True
                    else:
                        predict = False

                    # Check if update time is after current date. Only add to scheduler if after current timedate
                    if datetime.utcnow() < update_datetime:
                        # Schedule update job and add to list. The day is increased by 1, to make it indexed by 1, so 0 idx is day 1
                        new_job = self.sched.add_job(self._general_gc_update, run_date=update_datetime, args=[curr_gc, day + 1, timeslot, predict], id=f'day_{day}_ts_{timeslot}_update')
                        self.job_list.append(new_job)

                        log.info('Update Scheduled for day ' + str(day + 1) + ', timeslot: ' + str(timeslot) + ' at time: ' + str(update_datetime))
        except:
            tb = traceback.format_exc()
            log_exception('Failed to schedule GC updates', tb)

    def _day_2_update(self, gc_num):
        try:
            log.info('Running day 2 GC rank update...')

            # Remove all predictions for this GC day 1 (In case predictions were made before day 1 of GC)

            # Update db to ensure database is up to date, passing the day of the results
            self._full_gc_rank_update(1)

            # Update the day 0 table. (Assuming this function runs before overall rankings are updated 30 mins after reset time)
            self._init_day_0_gc_list(gc_num)

            # Get data from db and fills in the day 1 and day 2 predicted matches (Backfilling day 1)
            self._update_day_1_matches(gc_num)

            # Also update the predictions for day 2 aftwerwards
            # Get list of timeslots from db
            get_ts_list = select(self.timeslot_table.c.timeslot).where(self.timeslot_table.c.gc_available == True)

            with self.engine.connect() as conn:
                ts_list = conn.execute(get_ts_list).all()
                conn.commit()

            log.info('Day 2 full rank update complete (Reset time)')

            log.info('Starting GC match predictions for day 2...')
            for ts, in ts_list: 
                # Update ever ts in gc
                self._general_matchmaking(gc_num, 1, ts)
            
            log.info('GC matchmaking predictions complete')
        except:
            tb = traceback.format_exc()
            log_exception('Failed to complete update', tb)


    def _initial_gc_prediction(self, ts_guild_rank_list):
        log.info('Starting initial GC match predictions...')
        match_list = []
        # Takes a list of guilds in a ts and predicts day 1 matchups

        # Match each guild with the next guild in rankings
        # Get guilds in 1st, 3rd, 5th, places etc...
        list_a = ts_guild_rank_list[0::2]
        list_b = ts_guild_rank_list[1::2]

        log.info(f'list A:{len(list_a)}')
        log.info(f'list B:{len(list_b)}')

        # If there is an odd number of guilds, is the final guild is excluded from matching? Needs to be checked
        for guild_a, guild_b in itertools.zip_longest(list_a, list_b):
            # Only guild_b has a possibility of being None
            if guild_b is not None:
                pairing = {
                    'guilddataid': guild_a['guilddataid'],
                    'opponentguilddataid': guild_b['guilddataid'],
                    'gcday': guild_a['gcday'],
                    'gvgeventid': guild_a['gvgeventid']
                }

                alt_pairing = {
                    'guilddataid': guild_b['guilddataid'],
                    'opponentguilddataid': guild_a['guilddataid'],
                    'gcday': guild_b['gcday'],
                    'gvgeventid': guild_b['gvgeventid']
                }

                match_list.append(pairing)
                match_list.append(alt_pairing)
            else:
                pairing = {
                    'guilddataid': guild_a['guilddataid'],
                    'opponentguilddataid': None,
                    'gcday': guild_a['gcday'],
                    'gvgeventid': guild_a['gvgeventid']
                }
                match_list.append(pairing)

        log.info('Initial predictions complete')

        return match_list

    def _pre_gc_prediction(self, gc_num):
        # Function to run before day 1 of GC

        # Get the current guild list and timeslots

        # Run the initial matchmaking for each TS

        # Insert results into DB


    def _predict_all_ts_matches(self, full_guild_list, timeslots):
        # Filter the guild list and run the initial matchmaking function for each TS
        match_list = []

        # ITerate through each TS and filter guild list by the selected TS
        for gvgtimetype, in timeslots:
            # Define a filter function for the TS
            def _filter_func(curr_guild):
                # Unpack guild data tuple
                (gc_num, guild_id, guild_timetype, ranking) = curr_guild
                # Check if gvgtimetype matches the one in the outer loop
                if gvgtimetype == guild_timetype:
                    # Guild in the time slot
                    return True
                else:
                    return False

            # Perform the filter. It should preserve order
            filtered_list = filter(_filter_func, full_guild_list)

            converted_list = []
            for gc_num, guild_id, timetype, ranking  in filtered_list:
                new_dict = {
                    'gcday': 1, # This function is only ever used for day 1
                    'gvgeventid': gc_num,
                    'guilddataid': guild_id,
                    'ranking': ranking,
                    'gvgtimetype': timetype
                }
                converted_list.append(new_dict)

            # Pass the TS guild list into the initial matchmaking function
            matches = self._initial_gc_prediction(converted_list)
            log.info(f'Matched a total of {len(matches)} matches for time type: {gvgtimetype}')

            # Add to the match list
            match_list.extend(matches)

        log.info(f'Matchmaking complete. Number of matches calculated:{len(matches)}')
        return match_list

    def _update_day_1_matches(self, gc_num):
        log.info('Running day 1 match interpolation...')
        # Function to update the day 1 matchmaking list based on guilds participating in GC

        # Inner join day 0 (temp) table with guilds table on guilddataid to get only guilds participating in GC
        joined_table = self.gc_data_table.join(self.day_0_table, and_(self.day_0_table.c.guilddataid == self.gc_data_table.c.guilddataid, self.day_0_table.c.gvgeventid == self.gc_data_table.c.gvgeventid))
        # Statement to get the required data from the joined table. Order by ranking, get only data for the current GC
        participating_guilds_stmt = select(self.gc_data_table.c.gvgeventid, self.gc_data_table.c.guilddataid, self.gc_data_table.c.gvgtimetype, self.day_0_table.c.ranking).select_from(joined_table).where(self.gc_data_table.c.gvgeventid == gc_num, self.gc_data_table.c.gcday == 1).order_by(asc(self.day_0_table.c.ranking))

        # Get time slots participating in GC
        ts_statement = select(self.timeslot_table.c.gvgtimetype).where(self.timeslot_table.c.gc_available == True)

        log.info('Getting guilds participating in GC and timeslots...')
        with self.engine.connect() as conn:
            participating_guild_list = conn.execute(participating_guilds_stmt).all()

            timeslots = conn.execute(ts_statement).all()
            conn.commit()
        log.info(f'Retrieval successful. Guild list length:{len(participating_guild_list)}')
        
        match_list = []

        log.info('Filtering guilds for each timeslot and performing matchmaking...')
        match_list = self._predict_all_ts_matches(participating_guild_list, timeslots)

        # Update the db with the matched pairs
        insert_gc_matches = insert(self.match_table)
        update_gc_matches = {col.name: col for col in insert_gc_matches.excluded if col.name not in ('gcday', 'gvgeventid', 'guilddataid')}
        update_statement = insert_gc_matches.on_conflict_do_update(
            index_elements=['gcday', 'gvgeventid', 'guilddataid'], 
            set_=update_gc_matches
        )

        log.info('Inserting day 1 GC matches into DB')
        with self.engine.connect() as conn:
            conn.execute(update_statement, match_list)
            conn.commit()
        log.info('Insert successful')

        log.info('Day 1 matches update complete')


    def _general_gc_update(self, gc_num, day, timeslot, predict=True):
        try:
            log.info('Updating GC ' + str(gc_num) + ' ranks for day ' + str(day) + ', timeslot: ' + str(timeslot) + ' at time: ' + str(datetime.utcnow()))
            # Get the full guild rank data and insert to database
            self._full_gc_rank_update(day)
            log.info('Update Complete')


            log.info('Run GC matchmaking: ' + str(predict))

            # Check if matching function needs to be run
            if predict == True:
                # Run the matching function (Gets data from database)
                self._general_matchmaking(gc_num, day, timeslot)
        except:
            tb = traceback.format_exc()
            log_exception('General GC update failed.', tb)

    def _general_matchmaking(self, gc_num, day, timeslot):
        log.info('Running general matchmaking function for GC ' + str(gc_num) + ', predicting day ' + str(day + 1) + ' matches for timeslot: ' + str(timeslot))
        matched_list = []
        match_dict = {}
        predicted_match_list = []
        time_type = (2 ** (timeslot - 1))

        # Get gc data for day and timeslot (For predicting the following day)
        get_gc_data_statement = select(self.gc_data_table.c.gvgeventid, self.gc_data_table.c.gcday, self.gc_data_table.c.guilddataid, self.gc_data_table.c.point).where(self.gc_data_table.c.gvgeventid == gc_num, self.gc_data_table.c.gvgtimetype == time_type, self.gc_data_table.c.gcday == day).order_by(desc(self.gc_data_table.c.point), asc(self.gc_data_table.c.gvgeventrankingdataid))

        # Get the current match list from db
        joined_table = self.match_table.join(self.gc_data_table, and_(self.match_table.c.guilddataid == self.gc_data_table.c.guilddataid, self.match_table.c.gcday == self.gc_data_table.c.gcday, self.gc_data_table.c.gvgeventid == self.match_table.c.gvgeventid))
        get_gc_matches_statement = select(self.match_table.c.guilddataid, func.array_agg(self.match_table.c.opponentguilddataid)).select_from(joined_table).where(self.match_table.c.gvgeventid == gc_num, self.match_table.c.gcday <= day, self.gc_data_table.c.gvgtimetype == time_type).group_by(self.match_table.c.guilddataid)

        # Execute statements
        with self.engine.connect() as conn:
            ranking_tuple_list =  conn.execute(get_gc_data_statement).all()
            match_tuple_list = conn.execute(get_gc_matches_statement).all()
            conn.commit()

        #log.info(f'Match list: {str(match_tuple_list)}')
        log.info(f'Length of matched list from DB:{len(match_tuple_list)}')

        # Convert match list to dict containing array of guilds fought
        for guild_id, past_match_list in match_tuple_list:
            match_dict[guild_id] = past_match_list

        log.info(f'Length of guild list from DB: {len(ranking_tuple_list)}')

        predicted_match_list = self._predict_matchups_wrapper(match_dict, ranking_tuple_list)

        log.info(f'length of predictions: {len(predicted_match_list)}')

        # Can delete. Only for debugging
        seen_list = []
        for matchup in predicted_match_list:
            if matchup['guilddataid'] not in seen_list:
                seen_list.append(matchup['guilddataid'])
            else:
                dup_id = matchup['guilddataid']
                log.info(f'Duplicate guild id: {dup_id}')
                log.info(f'Data: {str(matchup)}')

        
        # Insert new predicted matches into database
        insert_gc_matches = insert(self.match_table).values(predicted_match_list)
        update_gc_matches = {col.name: col for col in insert_gc_matches.excluded if col.name not in ('gcday', 'gvgeventid', 'guilddataid')}
        update_statement = insert_gc_matches.on_conflict_do_update(
            index_elements=['gcday', 'gvgeventid', 'guilddataid'], 
            set_=update_gc_matches
        )

        log.info('Inserting new predicted matches into DB...')
        with self.engine.connect() as conn:
            conn.execute(update_statement)
            conn.commit()
        log.info('Insert successful')

        log.info('General matchmaking complete')

    def _predict_matchups_wrapper(self, guild_matches_dict, guild_list):
        matched_list = []
        predicted_match_list = []
        unmatched_nodes_list = []

        # Iterate through the guild list to match every guild
        for index in range(0, len(guild_list), 1):
            # Wrapper for recursive function. The function will append to the predicted_match_list passed in
            self._recurse_matchups_root(0, 1, guild_matches_dict, matched_list, predicted_match_list, guild_list[index::], unmatched_nodes_list)

            # Iterate through and check the match nodes added from the previous guild matchup until all are matched (unmatched_node_list may increase in length here)
            while len(unmatched_nodes_list) > 0:
                (index_a, index_b) = unmatched_nodes_list.pop(0)
                self._recurse_matchups_root(index_a, index_b, guild_matches_dict, matched_list, predicted_match_list, guild_list[index::], unmatched_nodes_list)

        return predicted_match_list

    # For use in calculating the indexes in the logic tree
    def _next_idx_generator(self, initial_idx):
        next_idx = initial_idx + 1
        while True:
            yield next_idx
            next_idx += 1

    def _recurse_matchups_root(self, index_a, index_b, guild_matches_dict, matched_list, predicted_match_list, guild_list, unmatched_nodes_list):
        gen = self._next_idx_generator(index_b)
        # Base case 1: End of even numbered list. No guilds left to match
        if index_a >= len(guild_list):
            # Last element of guild list
            return

        # Unpack tuple from list
        (gvgeventid, day, guilddataid, curr_point) = guild_list[index_a]

        # Base case 2: Already matched, return
        if guilddataid in matched_list:
            return

        # Base case 3: Odd numbered list. No guild to match with guid at index_a
        if index_b >= len(guild_list):
            # Add to predicted match list
            self._add_to_predicted_matches(predicted_match_list, matched_list, day, gvgeventid, guilddataid, None)
            return

        (gvgeventid, day, prospective_opp_id, opp_point) = guild_list[index_b]

        # Check if if guild at index_a can fight guild at index_b (while loop)
        valid_match = self._can_match(guilddataid, prospective_opp_id, matched_list, guild_matches_dict)
        remaining_node_list = []

        if valid_match:
            # Add to predicted match list
            self._add_to_predicted_matches(predicted_match_list, matched_list, day, gvgeventid, guilddataid, prospective_opp_id)


        # Repeat while guild at index_a is not matched
        while (not valid_match and index_b < len(guild_list) and index_a < len(guild_list)):
            prospective_opp_id = None

            # Calculate child nodes and queue the right child to node list
            child_node_list = self._calculate_children_node_indexes(gen, index_a, index_b)

            # Get the left child indexes
            (index_a, index_b) = child_node_list.pop(0)

            # Iterate through each remaining node and calculate their children nodes and enqueue to list
            if len(remaining_node_list) > 0:
                temp_node_list = []
                # Iterate through the nodes in the last level of the node list
                for node in remaining_node_list[-1]:
                    (curr_node_idx_a, curr_node_idx_b) = node
                    child_nodes = self._calculate_children_node_indexes(gen, curr_node_idx_a, curr_node_idx_b)
                    temp_node_list.extend(child_nodes)
                # Add the nw level to the node list
                remaining_node_list.append(temp_node_list)

            # Add the right child to the remaining node list
            remaining_node_list.append(child_node_list)

            (gvgeventid, day, guilddataid, curr_point) = guild_list[index_a]

            # Check if new match is possible
            if index_b < len(guild_list):
                (gvgeventid, day, prospective_opp_id, opp_point) = guild_list[index_b]
                valid_match = self._can_match(guilddataid, prospective_opp_id, matched_list, guild_matches_dict)
            else:
                #self._add_to_predicted_matches(predicted_match_list, matched_list, day, gvgeventid, guilddataid, None)
                prospective_opp_id = None
                valid_match = True

            if valid_match:
                # If can match, add to predicted list and iterate through the right node list (for each loop)
                self._add_to_predicted_matches(predicted_match_list, matched_list, day, gvgeventid, guilddataid, prospective_opp_id)

                for level_num, tree_level in enumerate(remaining_node_list):
                    for node in tree_level:
                        # Get each remaining node in list, check if the guilds contained in the node can match
                        (child_idx_a, child_idx_b) = node
                        # Get the guilds (Reusing variables. Bad practice I know)
                        if child_idx_a < len(guild_list):
                            (gvgeventid, day, guilddataid, curr_point) = guild_list[child_idx_a]
                            if child_idx_b < len(guild_list):
                                (gvgeventid, day, prospective_opp_id, opp_point) = guild_list[child_idx_b]

                                # Check if can match
                                if self._can_match(guilddataid, prospective_opp_id, matched_list, guild_matches_dict):
                                    # Match the guilds
                                    self._add_to_predicted_matches(predicted_match_list, matched_list, day, gvgeventid, guilddataid, prospective_opp_id)
                                elif level_num == len(remaining_node_list) - 1:
                                    # Cannot match and is at last level of the tree
                                    # Add to list of unmatched nodes to run through another pass of matching function later
                                    unmatched_nodes_list.append(node)

                                # If none of the if conditions met, then it is a non-leaf node that cannot match. Do nothing.
                            else:
                                # index b out of range, Match guild at index a with None (Not sure what happens in this situation yet)
                                self._add_to_predicted_matches(predicted_match_list, matched_list, day, gvgeventid, guilddataid, None)

        return unmatched_nodes_list

    def _calculate_children_node_indexes(self, gen, index_a, index_b):
        # Convert form 0 indexed to 1 indexed for working
        child_index_tuples = []

        left_child_index_a = index_a
        left_child_index_b = next(gen)

        right_child_index_a = index_b
        right_child_index_b =  next(gen) # Should be +1 from left_child_index_b

        # Add to list
        left_node = (left_child_index_a, left_child_index_b)
        right_node = (right_child_index_a, right_child_index_b)

        child_index_tuples.append(left_node)
        child_index_tuples.append(right_node)

        return child_index_tuples


    def _can_match(self, guilddataid, prospective_opp_id, matched_list, guild_matches_dict):
        if guilddataid not in matched_list and prospective_opp_id not in matched_list and guilddataid in guild_matches_dict and prospective_opp_id not in guild_matches_dict[guilddataid]:
            # Have not fought before. Valid match
            return True
        elif (guilddataid not in matched_list and prospective_opp_id not in matched_list and guilddataid not in guild_matches_dict):
            # Not in guild matches dict. Probably left out and doesn't have a match
            return True

        return False

    def _add_to_predicted_matches(self, predicted_match_list, matched_list, day, gvgeventid, guilddataid, prospective_opp_id):
        # Add to match list
        new_match = {
            'gcday': day + 1,
            'gvgeventid': gvgeventid,
            'guilddataid': guilddataid,
            'opponentguilddataid': prospective_opp_id
        }

        alt_match = {
            'gcday': day + 1,
            'gvgeventid': gvgeventid,
            'opponentguilddataid': guilddataid,
            'guilddataid': prospective_opp_id
        }

        predicted_match_list.append(new_match)
        matched_list.append(guilddataid)

        if prospective_opp_id is not None:
            matched_list.append(prospective_opp_id)
            predicted_match_list.append(alt_match)

    def _init_day_0_gc_list(self, gc_num):
        day_0_list = []

        log.info('Getting guild list from DB...')
        # Get full list of guilds from db
        select_guilds_stmt = select(self.guild_table.c.guilddataid, self.guild_table.c.gvgtimetype, self.guild_table.c.ranking)

        with self.engine.connect() as conn:
            guild_list = conn.execute(select_guilds_stmt).all()
            conn.commit()
        log.info('Retrieval successful')

        for guild_data_id, gvg_time_type, ranking in guild_list:
            # Convert to list of dicts with gc number included
            guild_dict = {
                'guilddataid': guild_data_id,
                'gvgeventid': gc_num,
                'gvgtimetype': gvg_time_type,
                'ranking': ranking
            }

            day_0_list.append(guild_dict)


        # Insert the list into the temp/day 0 table
        insert_day_0 = insert(self.day_0_table).values(day_0_list)
        update_day_0 = {col.name: col for col in insert_day_0.excluded if col.name not in ('guilddataid')}
        update_statement = insert_day_0.on_conflict_do_update(
            index_elements=['guilddataid'], 
            set_=update_day_0
        )

        log.info('Inserting day 0 guilds into DB...')
        with self.engine.connect() as conn:
            conn.execute(update_statement)
            conn.commit()
        log.info('Insert successful')