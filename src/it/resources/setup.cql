/*
  Copyright 2016 Tuplejump

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

/* Always clean to not run against existing data. */
DROP KEYSPACE IF EXISTS githubstats;

DROP KEYSPACE IF EXISTS test;

CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS test.kv (
  key text,
  value int,
  PRIMARY KEY (key));

CREATE TABLE test.playlists (
  id bigint,
  song_order int,
  song_id bigint,
  title text,
  album text,
  artist text,
  PRIMARY KEY  (id, song_order ) );

INSERT INTO test.playlists (id, song_order, song_id, title, artist, album)
  VALUES (6, 4, 66, 'Ojo Rojo', 'Fu Manchu', 'No One Rides for Free');

INSERT INTO test.playlists (id, song_order, song_id, title, artist, album)
  VALUES (6, 1, 44, 'La Grange', 'ZZ Top', 'Tres Hombres');

INSERT INTO test.playlists (id, song_order, song_id, title, artist, album)
  VALUES (2, 2, 469, 'Moving in Stereo', 'Fu Manchu', 'We Must Obey');

INSERT INTO test.playlists (id, song_order, song_id, title, artist, album)
  VALUES (2, 3, 49077, 'Outside Woman Blues', 'Back Door Slam', 'Roll Away');

CREATE KEYSPACE githubstats WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE githubstats.monthly_commits (
  user VARCHAR,
  commits INT,
  month INT,
  year INT,
  PRIMARY KEY ((user), year, month)
) WITH CLUSTERING ORDER BY (year DESC, month DESC);

CREATE TABLE IF NOT EXISTS test.event_store(
    app_id text,
    event_type text,
    subscription_type text,
    event_ts timestamp,
    PRIMARY KEY((app_id, event_type), event_ts)
    );