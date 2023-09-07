CREATE TABLE serializable_tab1 (
    id bigint NOT NULL
);

CREATE TABLE serializable_tab2 (
    id bigint NOT NULL,
    user_id bigint
);

ALTER TABLE ONLY serializable_tab1
    ADD CONSTRAINT user_constraint UNIQUE (id);

ALTER TABLE ONLY serializable_tab2
    ADD CONSTRAINT assignment_constraint UNIQUE (user_id);

ALTER TABLE ONLY serializable_tab2
    ADD CONSTRAINT user_assignments_user_id_fkey FOREIGN KEY (user_id) REFERENCES serializable_tab1(id) ON DELETE CASCADE;


INSERT INTO serializable_tab1 (id) VALUES ('12345');
INSERT INTO serializable_tab2 (id, user_id) VALUES ('1', '12345');

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
DELETE FROM serializable_tab1 where id = '12345';
SELECT * FROM serializable_tab1;
ROLLBACK;

SELECT * FROM serializable_tab1;

DROP TABLE serializable_tab2;
DROP TABLE serializable_tab1;

