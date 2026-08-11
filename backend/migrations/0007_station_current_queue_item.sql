-- Expand-only queue cursor. Once current_queue_cursor_format is 1, this schema
-- must only be moved forward or restored from a pre-cutover backup.
ALTER TABLE stations
    ADD COLUMN IF NOT EXISTS current_queue_item_id UUID,
    ADD COLUMN IF NOT EXISTS consumed_queue_item_ids UUID[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS current_queue_cursor_format SMALLINT NOT NULL DEFAULT 0;

ALTER TABLE stations
    DROP CONSTRAINT IF EXISTS stations_current_queue_cursor_format_check;
ALTER TABLE stations
    ADD CONSTRAINT stations_current_queue_cursor_format_check
    CHECK (current_queue_cursor_format IN (0, 1));

COMMENT ON COLUMN stations.current_queue_cursor_format IS
    'Durable cutover marker: format 1 cannot be downgraded in place';
