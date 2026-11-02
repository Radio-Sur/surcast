-- Only two account types remain: administrator and demo. Legacy manager and
-- viewer accounts were staff accounts, so they keep their access as admins.
UPDATE users SET role = 'admin' WHERE role NOT IN ('admin', 'demo');

-- Fail closed if a role is ever omitted: the demo sandbox is restricted and
-- its sign-in stays blocked until isolation is provisioned.
ALTER TABLE users ALTER COLUMN role SET DEFAULT 'demo';

ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check;
ALTER TABLE users ADD CONSTRAINT users_role_check CHECK (role IN ('admin', 'demo'));
