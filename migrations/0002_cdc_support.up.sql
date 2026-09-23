-- Support for change data capture into the search indexes.

-- A link to the document. The CDC mapping indexes it as a keyword and returns
-- it with search results.
ALTER TABLE documents ADD COLUMN IF NOT EXISTS url TEXT;

-- version must increase on every change, including when a deleted id is
-- inserted again: OpenSearch keeps the highest version it has seen and ignores
-- any write that does not exceed it, so an UPDATE that forgot to bump version
-- would silently never reach the keyword index. Rather than trusting every
-- writer, the database assigns it from one sequence shared by all rows.
CREATE SEQUENCE IF NOT EXISTS documents_version_seq OWNED BY documents.version;
SELECT setval('documents_version_seq', max_version)
FROM (SELECT MAX(version) AS max_version FROM documents) existing
WHERE max_version IS NOT NULL;
ALTER TABLE documents ALTER COLUMN version SET DEFAULT nextval('documents_version_seq');

CREATE OR REPLACE FUNCTION documents_stamp_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- Owned by the database: any value the writer supplied is replaced.
    NEW.version := nextval('documents_version_seq');
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS documents_stamp_change ON documents;
CREATE TRIGGER documents_stamp_change
    BEFORE INSERT OR UPDATE ON documents
    FOR EACH ROW EXECUTE FUNCTION documents_stamp_change();
