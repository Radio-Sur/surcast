-- Icecast: enable managed auto-start on boot by default.
ALTER TABLE icecast_settings ALTER COLUMN enabled SET DEFAULT true;
UPDATE icecast_settings SET enabled = true WHERE id = '00000000-0000-0000-0000-000000000001';

-- Station transitions: default to autocue (cue-point driven crossfade).
ALTER TABLE stations ALTER COLUMN transition_mode SET DEFAULT 'autocue';
UPDATE stations SET transition_mode = 'autocue' WHERE transition_mode = 'crossfade';