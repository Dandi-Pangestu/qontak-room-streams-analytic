SELECT count(*) FROM rooms;
SELECT count(*) FROM rooms WHERE status='assigned' AND id NOT IN ('d703e2a4-25f1-4111-933f-f7dee946cb62', 'b0d6892b-fa66-47d7-8411-70ee717c3bb7', 'ad777d48-e1a5-4915-b72c-075eef933de5');
SELECT count(*) FROM rooms WHERE status='unassigned' AND id NOT IN ('d703e2a4-25f1-4111-933f-f7dee946cb62', 'b0d6892b-fa66-47d7-8411-70ee717c3bb7', 'ad777d48-e1a5-4915-b72c-075eef933de5');
SELECT count(*) FROM rooms WHERE status='resolved' AND id NOT IN ('d703e2a4-25f1-4111-933f-f7dee946cb62', 'b0d6892b-fa66-47d7-8411-70ee717c3bb7', 'ad777d48-e1a5-4915-b72c-075eef933de5');

ALTER TABLE custom_views REPLICA IDENTITY FULL;

SELECT relname, relreplident
FROM pg_class
WHERE relname = 'custom_views';

SELECT * FROM pg_replication_slots;
SELECT * FROM pg_stat_activity WHERE pid = 40;