-- Accounts are identified by username alone; the redundant display name is gone.
ALTER TABLE users DROP COLUMN IF EXISTS name;
