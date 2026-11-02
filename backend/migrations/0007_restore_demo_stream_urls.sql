-- Demo uses the source station's configured output, not a generated mount.
-- Only replace legacy generated values; preserve subsequent custom settings.
DO $$
DECLARE sandbox RECORD; station JSONB; sandbox_schema TEXT;
BEGIN
    FOR sandbox IN SELECT generation, seed FROM demo_sandboxes LOOP
        sandbox_schema := 'demo_' || replace(sandbox.generation::text, '-', '');
        IF to_regclass(format('%I.stations', sandbox_schema)) IS NOT NULL THEN
            FOR station IN SELECT value FROM jsonb_array_elements(sandbox.seed->'stations') LOOP
                EXECUTE format('UPDATE %I.stations SET stream_url=$1 WHERE id=$2 AND stream_url=$3', sandbox_schema)
                    USING station->>'stream_url', (station->>'id')::uuid,
                          'surcast-demo-' || replace(sandbox.generation::text, '-', '') || '.mp3';
            END LOOP;
        END IF;
    END LOOP;
END $$;
