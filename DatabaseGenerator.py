from sqlalchemy import create_engine, Table, MetaData, select, delete
from sqlalchemy.dialects.postgresql import insert
import psycopg2
from dotenv import load_dotenv
import os
import random

load_dotenv()

class DatabaseGenerator():
    def __init__(self):
        self.engine = create_engine(os.getenv('POSTGRES_URL'))
        self.metadata = MetaData()
        self.guild_table = Table(
            'guilds', 
            self.metadata, 
            autoload_with=self.engine
        )

        self.player_data_table = Table(
            'player_data', 
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
        pass

    def regenerate_gc_data(self):
        # Get data from database
        init_data = self._get_day_0_data()


        # Call generator function with data
        generated_data = self._generate_gc_day_data(init_data)

        # Insert back into database
        self._insert_gc_data(generated_data)

    def _generate_gc_day_data(self, day_0_data):
        generated_data = []
        # Loop for each timeslot
        for ts in [4, 8, 16, 32, 64, 128, 256, 512, 2048, 4096]:
            print(ts)
            # Filter data by timeslot
            ts_guild_list = [guild for guild in day_0_data if guild[3] in [ts]]
            print(ts_guild_list)
            # Iterate through each guild in timeslot
            for guild in ts_guild_list:
                # Loop for each day of GC
                for day in range(1, 7):
                    new_row = {
                        'gvgeventid': guild[0],
                        'gcday': day,
                        'guilddataid': guild[1],
                        'point': random.randint(0, 1000000000000),
                        'gvgtimetype': guild[3]
                    }
                    generated_data.append(new_row)



        return generated_data

    def _get_day_0_data(self):
        get_statement = select(self.day_0_table.c.gvgeventid, self.day_0_table.c.guilddataid, self.day_0_table.c.ranking, self.day_0_table.c.gvgtimetype)
        
        # Execute command
        with self.engine.connect() as conn:
            init_data = conn.execute(get_statement).all()
            conn.commit()

        return init_data

    def _clear_match_data(self):
        del_statement = delete(self.match_table).where(self.match_table.c.gvgeventid == 0)
        
        # Execute command
        with self.engine.connect() as conn:
            conn.execute(del_statement)
            conn.commit()
        

    def _insert_gc_data(self, data):
        insert_gc_ranks = insert(self.gc_data_table)
        update_gc_matches = {col.name: col for col in insert_gc_ranks.excluded if col.name not in ('gcday', 'gvgeventid', 'guilddataid')}
        update_statement = insert_gc_ranks.on_conflict_do_update(
            index_elements=['gcday', 'gvgeventid', 'guilddataid'], 
            set_=update_gc_matches
        )
        
        # Execute command
        with self.engine.connect() as conn:
            conn.execute(update_statement, data)
            conn.commit()

    def _copy_guilds_to_tmp(self):
        copied_data = []
        # Copy guilds data to tmp table

        # Get data from guild table
        select_stmt = select(self.guild_table.c.guilddataid, self.guild_table.c.ranking, self.guild_table.c.gvgtimetype)

        with self.engine.connect() as conn:
            data = conn.execute(select_stmt).all()
            conn.commit()

        # Convert data to dict list
        for guilddataid, ranking, gvgtimetype in data:
            new_dict = {
                'gvgeventid': 0,
                'guilddataid': guilddataid,
                'ranking': ranking,
                'gvgtimetype': gvgtimetype
            }
            copied_data.append(new_dict)

        # Insert into temp table
        insert_stmt = insert(self.day_0_table)
        update_tmp = {col.name: col for col in insert_stmt.excluded if col.name not in ('guilddataid')}
        update_statement = insert_stmt.on_conflict_do_update(
            index_elements=['guilddataid'], 
            set_=update_tmp
        )
        with self.engine.connect() as conn:
            conn.execute(update_statement, copied_data)
            conn.commit()