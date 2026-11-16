-- Fresh installation; run once in an empty destination.
CREATE SCHEMA hrt;

CREATE TABLE hrt.unit_type (
    unit_type_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    type VARCHAR(50) NOT NULL,
    CONSTRAINT pk_unit_type PRIMARY KEY (unit_type_id),
    CONSTRAINT uq_unit_type UNIQUE (type)
);

CREATE TABLE hrt.component (
    component_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    name VARCHAR(255) NOT NULL,
    unit_type_id BIGINT NOT NULL,
    unit_amount DECIMAL(12, 3) NOT NULL,
    CONSTRAINT pk_component PRIMARY KEY (component_id),
    CONSTRAINT fk_component_unit_type FOREIGN KEY (unit_type_id)
        REFERENCES hrt.unit_type (unit_type_id),
    CONSTRAINT uq_component UNIQUE (name, unit_type_id, unit_amount),
    CONSTRAINT ck_component_amount CHECK (unit_amount > 0)
);
-- Phase 1 extension: these tables are new, not recovered from the transcript.
CREATE TABLE hrt.subject (
    subject_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    subject_code VARCHAR(100) NOT NULL,
    CONSTRAINT pk_subject PRIMARY KEY (subject_id),
    CONSTRAINT uq_subject_code UNIQUE (subject_code)
);

-- Each row records one actual administration, not a planned dose.
-- amount is expressed in the referenced component's unit; never package count.
-- occurred_at is UTC by application convention; intake_date is subject-local.
CREATE TABLE hrt.intake (
    intake_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    event_key VARCHAR(100) NOT NULL,
    subject_id BIGINT NOT NULL,
    component_id BIGINT NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    intake_date DATE NOT NULL,
    amount DECIMAL(12, 3) NOT NULL,
    notes VARCHAR(1000),
    CONSTRAINT pk_intake PRIMARY KEY (intake_id),
    CONSTRAINT uq_intake_event UNIQUE (event_key),
    CONSTRAINT fk_intake_subject FOREIGN KEY (subject_id)
        REFERENCES hrt.subject (subject_id),
    CONSTRAINT fk_intake_component FOREIGN KEY (component_id)
        REFERENCES hrt.component (component_id),
    CONSTRAINT ck_intake_amount CHECK (amount > 0)
);
