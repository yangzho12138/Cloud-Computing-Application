DROP VIEW IF EXISTS "powers";
CREATE VIEW "powers" (
  id VARCHAR PRIMARY KEY,
  "personal"."hero" VARCHAR,
  "personal"."power" VARCHAR,
  "professional"."name" VARCHAR,
  "professional"."xp" VARCHAR,
  "custom"."color" VARCHAR
)AS SELECT * FROM "powers";


SELECT p1."professional"."name" AS "Name1", p2."professional"."name" AS "Name2", p1."personal"."power" AS "Power" FROM "powers" AS p1, "powers" AS p2 WHERE p1."personal"."power" = p2."personal"."power" AND p1."personal"."hero" = 'yes' AND p2."personal"."hero" = 'yes';