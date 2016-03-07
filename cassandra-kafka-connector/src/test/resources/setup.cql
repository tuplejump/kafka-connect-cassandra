CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

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
