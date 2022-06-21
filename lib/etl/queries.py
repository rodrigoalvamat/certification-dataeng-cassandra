# SELECT QUERIES

select_songplay_by_session = """
SELECT artist, song, length
FROM songplayBySession
WHERE sessionId = 338 AND itemInSession = 4
"""

select_songplay_by_user = """
SELECT artist, song, firstName, lastName
FROM songplayByUser
WHERE userId = 10 AND sessionId = 182
ORDER BY itemInSession ASC
"""

select_songplay_by_song = """
SELECT firstName, lastName
FROM songplayBySong
WHERE song = 'All Hands Against His Own'
"""

# CREATE TABLE QUERIES

table_songplay_by_session = """
CREATE TABLE IF NOT EXISTS songplayBySession
(artist text, song text, length float,
sessionId int, itemInSession int,
PRIMARY KEY ((sessionId, itemInSession)))
"""

table_songplay_by_user = """
CREATE TABLE IF NOT EXISTS songplayByUser
(artist text, song text, firstName text, lastName text,
userId int, sessionId int, itemInSession int,
PRIMARY KEY ((userId, sessionId), itemInSession))
"""

table_songplay_by_song = """
CREATE TABLE IF NOT EXISTS songplayBySong
(song text, sessionId int, itemInSession int,
firstName text, lastName text,
PRIMARY KEY (song, sessionId, itemInSession))
"""

# DROP TABLE QUERIES

drop_songplay_by_session = "DROP TABLE IF EXISTS songplayBySession"

drop_songplay_by_user = "DROP TABLE IF EXISTS songplayByUser"

drop_songplay_by_song = "DROP TABLE IF EXISTS songplayBySong"

# QUERY LISTS

select_queries = [select_songplay_by_session,
                  select_songplay_by_user, select_songplay_by_song]

create_table_queries = [table_songplay_by_session,
                        table_songplay_by_user, table_songplay_by_song]

drop_table_queries = [drop_songplay_by_session,
                drop_songplay_by_user, drop_songplay_by_song]
