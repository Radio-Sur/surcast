-- The control plane lives in the main database. Each sandbox has a separate
-- schema and music directory; its seed is immutable until reconfigured.
CREATE TABLE demo_sandboxes (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    generation UUID NOT NULL,
    seed JSONB NOT NULL,
    ready BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
