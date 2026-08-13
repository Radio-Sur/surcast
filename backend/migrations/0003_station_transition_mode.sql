-- Station transition mode: how song-to-song transitions are performed.
-- 'crossfade' (default, naive duration-based fade), 'autocue' (use autocue
-- cue points), or 'off' (no transition).

ALTER TABLE stations ADD COLUMN IF NOT EXISTS transition_mode TEXT NOT NULL DEFAULT 'crossfade';
