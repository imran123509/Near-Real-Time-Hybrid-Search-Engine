DROP TRIGGER IF EXISTS documents_stamp_change ON documents;
DROP FUNCTION IF EXISTS documents_stamp_change();
ALTER TABLE documents ALTER COLUMN version DROP DEFAULT;
DROP SEQUENCE IF EXISTS documents_version_seq;
ALTER TABLE documents DROP COLUMN IF EXISTS url;
