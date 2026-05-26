--Go to Warehouse table
\c warehouse

--1. vendor_table
CREATE TABLE vendor_table(
    vendor_id SMALLINT PRIMARY KEY,
    vendor_name VARCHAR(50) NOT NULL
);

INSERT INTO vendor_table VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'Curb Mobility, LLC'),
(6, 'Myle Technologies Inc.'),
(7, 'Helix');

--2. trip_type_table (for Green Taxi)
CREATE TABLE trip_type_table(
    trip_type_code SMALLINT PRIMARY KEY,
    trip_type VARCHAR(15) NOT NULL
);

INSERT INTO trip_type_table VALUES
(1, 'Street-Hail'),
(2, 'Dispatch');

--3. rate_code_table
CREATE TABLE rate_code_table(
    rate_code_id SMALLINT PRIMARY KEY,
    rate_code_zone VARCHAR(20) NOT NULL
);

INSERT INTO rate_code_table VALUES
(1, 'Standard Rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau/Westchester'),
(5, 'Negotiated Fare'),
(6, 'Group Ride'),
(99, 'Null/Unknown');

--4. payment_table
CREATE TABLE payment_table(
    payment_code SMALLINT PRIMARY KEY,
    payment_type VARCHAR(16) NOT NULL
);

INSERT INTO payment_table VALUES
(0, 'Flex Fare Trip'),
(1, 'Credit Card'),
(2, 'Cash'),
(3, 'No Charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided Trip');

--5. location_based_atributes
CREATE TABLE location_based_attributes(
    location_id SMALLINT PRIMARY KEY,
    location_borough VARCHAR(15) NOT NULL,
    location_zone VARCHAR(50) NOT NULL,
    location_service_zone VARCHAR(15) NOT NULL
);

COPY location_based_attributes(
    location_id,
    location_borough,
    location_zone,
    location_service_zone
)
FROM '/docker-entrypoint-initdb.d/taxi_zone_lookup.csv'
WITH (FORMAT csv, HEADER true);

--6. hvfhs_table
CREATE TABLE hvfhs_table(
    hvfhs_license_num VARCHAR(6) PRIMARY KEY,
    hvfhs_provider VARCHAR(6) NOT NULL
);

INSERT INTO hvfhs_table VALUES
('HV0002', 'Juno'),
('HV0003', 'Uber'),
('HV0004', 'Via'),
('HV0005', 'Lyft');

--7. base_table (Import CSV to temp table, then select only rows required for analysis)
CREATE TABLE base_table(
    base_num VARCHAR(6) PRIMARY KEY,
    base_name VARCHAR(128) NOT NULL,
    base_type VARCHAR(40) NOT NULL
);

CREATE TEMPORARY TABLE base_temp_table(
    license_number VARCHAR(6) PRIMARY KEY,
    base_name VARCHAR(60),
    telephone_num TEXT,
    shl_endorsed TEXT,
    wheelchair TEXT,
    building TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    type_of_base VARCHAR(30),
    longitude TEXT,
    latitude TEXT,
    last_date TEXT,
    last_time TEXT
);

COPY base_temp_table
FROM '/docker-entrypoint-initdb.d/current_bases.csv'
WITH (FORMAT csv, HEADER true);

INSERT INTO base_table (base_num, base_name, base_type)
SELECT license_number, base_name, type_of_base
FROM base_temp_table;

DROP TABLE base_temp_table;
