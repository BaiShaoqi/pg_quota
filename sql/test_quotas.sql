
--
CREATE TABLE qt (t text);

INSERT INTO qt SELECT repeat('x', 100) FROM generate_series(1, 100000);

-- Display the table size.
select pg_size_pretty(pg_total_relation_size('qt'));


-- Set a quota for the relation.
INSERT INTO quota.config VALUES ('qt'::regclass, pg_size_bytes('20 MB'));

-- Wait a little, to give the worker a chance to pick up the new quota.
select pg_sleep(5);

SELECT tablename,
       pg_size_pretty(space_used) as used,
       pg_size_pretty(quota) as quota
FROM quota.status;

-- Now insert enough data that the quota is exceeded.
INSERT INTO qt SELECT repeat('x', 100) FROM generate_series(1, 100000);

-- and again wait a little, so that the worker picks up the new file size
select pg_sleep(5);

SELECT tablename,
       pg_size_pretty(space_used) as used,
       pg_size_pretty(quota) as quota
FROM quota.status;

-- Try to insert again. This should fail, because the quota is exceeded.
INSERT INTO qt SELECT repeat('x', 100) FROM generate_series(1, 100000);


-- Free up the space, by truncating the table. Now it should work again.
TRUNCATE qt;

-- and again wait a little, so that the worker picks up the new file size
select pg_sleep(5);

SELECT tablename,
       pg_size_pretty(space_used) as used,
       pg_size_pretty(quota) as quota
FROM quota.status;

INSERT INTO qt SELECT repeat('x', 100) FROM generate_series(1, 100000);
