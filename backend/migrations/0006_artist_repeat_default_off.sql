-- Change defaults only; preserve explicitly saved settings.
ALTER TABLE station_auto_fill ALTER COLUMN avoid_artist_repeat SET DEFAULT false;

-- Existing sandbox schemas are isolated from public migrations.
DO $$
DECLARE sandbox_schema TEXT;
BEGIN
    FOR sandbox_schema IN
        SELECT n.nspname
        FROM pg_namespace n JOIN pg_class c ON c.relnamespace = n.oid
        WHERE n.nspname ~ '^demo_[0-9a-f]{32}$'
          AND c.relname = 'station_auto_fill' AND c.relkind = 'r'
    LOOP
        EXECUTE format('ALTER TABLE %I.station_auto_fill ALTER COLUMN avoid_artist_repeat SET DEFAULT false', sandbox_schema);
    END LOOP;
END $$;
