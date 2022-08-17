CREATE TABLE "visa_info" (
  "v_id" int PRIMARY KEY,
  "state" varchar,
  "visatype" varchar,
  "visa_category" varchar,
  "cicid" double
);

CREATE TABLE "arrival_info" (
  "cicid" double PRIMARY KEY,
  "arrival_dt" date,
  "dep_dt" date,
  "arr_year" int,
  "arr_month" int,
  "arr_dayofmonth" int,
  "arr_weekofyear" int,
  "arr_weekday" int,
  "days_of_stay" int,
  "state" varchar,
  "state_code" varchar,
  "city" varchar,
  "gender" varchar,
  "birthyear" double
);

CREATE TABLE "global_temperature_us" (
  "date" date PRIMARY KEY,
  "average_temp" double,
  "state" varchar
);

CREATE TABLE "state_mapping" (
  "code" varchar PRIMARY KEY,
  "state" varchar
);

CREATE TABLE "airport_info" (
  "airport_ident" varchar PRIMARY KEY,
  "airport_name" varchar,
  "iata_code" varchar,
  "state" varchar,
  "state_code" varchar
);

CREATE TABLE "us_demographics" (
  "demog_id" int PRIMARY KEY,
  "city" varchar,
  "state" varchar,
  "state_code" varchar,
  "male_pop" bigint,
  "female_pop" bigint,
  "total_pop" bigint,
  "race" varchar
);

CREATE TABLE "imm_dynamics_us" (
  "imm_dyn_id" int PRIMARY KEY,
  "cicid" double,
  "arrival_dt" date,
  "state" varchar,
  "visa_category" varchar,
  "average_temp" double,
  "state_code" varchar
);

ALTER TABLE "visa_info" ADD FOREIGN KEY ("cicid") REFERENCES "arrival_info" ("cicid");

ALTER TABLE "airport_info" ADD FOREIGN KEY ("state_code") REFERENCES "state_mapping" ("code");

ALTER TABLE "us_demographics" ADD FOREIGN KEY ("state_code") REFERENCES "state_mapping" ("code");

ALTER TABLE "imm_dynamics_us" ADD FOREIGN KEY ("cicid") REFERENCES "arrival_info" ("cicid");

ALTER TABLE "imm_dynamics_us" ADD FOREIGN KEY ("arrival_dt") REFERENCES "global_temperature_us" ("date");

ALTER TABLE "imm_dynamics_us" ADD FOREIGN KEY ("state") REFERENCES "global_temperature_us" ("state");

ALTER TABLE "imm_dynamics_us" ADD FOREIGN KEY ("state_code") REFERENCES "state_mapping" ("code");
