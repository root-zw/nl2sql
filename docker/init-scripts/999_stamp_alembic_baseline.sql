-- Docker 首次初始化后，为当前基线打上 Alembic 版本号。
-- 仅用于 /docker-entrypoint-initdb.d 首次建库场景，避免后续 Alembic 无法识别历史基线。

CREATE TABLE IF NOT EXISTS alembic_version (
    version_num VARCHAR(32) NOT NULL PRIMARY KEY
);

DELETE FROM alembic_version;

INSERT INTO alembic_version (version_num)
VALUES ('20260414_0001');
