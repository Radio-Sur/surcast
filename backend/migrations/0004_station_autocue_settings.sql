-- Maximum crossfade length (ms) used in 'autocue' transition mode, capping
-- the natural fade derived from cue points (cue_out - cross_start_next).
-- 0 disables the overlap in autocue mode.

ALTER TABLE stations ADD COLUMN IF NOT EXISTS autocue_fade_max_ms INTEGER NOT NULL DEFAULT 5000;
