CREATE TABLE pipelines (
                           id BIGSERIAL PRIMARY KEY,
                           status VARCHAR(32) NOT NULL,
                           created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                           finished_at TIMESTAMPTZ,
                           updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipeline_tasks (
                                id BIGSERIAL PRIMARY KEY,
                                pipeline_id BIGINT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
                                source_url TEXT NOT NULL,
                                details JSONB,
                                status VARCHAR(32) NOT NULL,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipeline_stage_results (
                                        id BIGSERIAL PRIMARY KEY,
                                        task_id BIGINT NOT NULL REFERENCES pipeline_tasks(id) ON DELETE CASCADE,
                                        stage VARCHAR(64) NOT NULL,
                                        status VARCHAR(32) NOT NULL,
                                        attempt INT NOT NULL DEFAULT 1 CHECK (attempt > 0),
                                        details JSONB NOT NULL DEFAULT '{}'::jsonb,
                                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);