-- Optimize this query
explain analyze 
SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE date_trunc('year',"date") = '2009-01-01';

-- Optimized
explain analyze
SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE "date" >= '2009-01-01' and "date" < '2010-01-01';

-- Optimize this query
explain analyze
SELECT s.track_id,
	a.track_name,
	track_album_id,
	track_popularity
FROM homework_3.spotify_songs_more_artists a 
JOIN homework_3.spotify_songs s 
	ON a.track_id = s.track_id
WHERE track_artist = 'Sia'
UNION ALL
SELECT s.track_id,
       a.track_name,
       track_album_id,
       track_popularity
FROM homework_3.spotify_songs_more_artists a
JOIN homework_3.spotify_songs s 
	ON a.track_id = s.track_id
WHERE duration_ms::int between 299900 and 300827;

-- Optimized
explain analyze
with cte as (
	SELECT track_id, track_name
	from homework_3.spotify_songs_more_artists
	where track_artist = 'Sia' or duration_ms between '299900' and '300827'
)
SELECT s.track_id,
	a.track_name,
	s.track_album_id,
	s.track_popularity
FROM cte a 
JOIN homework_3.spotify_songs s ON a.track_id = s.track_id;

-- Optimized but without duplicated records so instead of 11 million records 46 thousand records
with cte as (
	SELECT distinct track_id, track_name
	from homework_3.spotify_songs_more_artists
	where track_artist = 'Sia' or duration_ms::int between 299900 and 300827
)
SELECT s.track_id,
	a.track_name,
	s.track_album_id,
	s.track_popularity
FROM cte a 
JOIN homework_3.spotify_songs s ON a.track_id = s.track_id;
