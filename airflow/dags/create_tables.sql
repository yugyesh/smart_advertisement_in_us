CREATE TABLE IF NOT EXISTS public.demographics (
	demographic_id bigint not null,
	city varchar(100),
	state varchar(100),
	american_indian_alaska_native bigint,
	asian bigint,
	black_african_american bigint,
	hispanic_latino bigint,
	white bigint,
	male_population bigint,
	total_population bigint
	female_population bigint,
);

CREATE TABLE IF NOT EXISTS public.immigration (
	cicid bigint,
	origin_city varchar(100),
	traveled_from varchar(100),
	arrived_city varchar(100),
	us_address varchar(100),
	arrival_date date,
	departure_date date,
	transportation_mode varchar(100),
	age integer,
	gender varchar(15),
	visa_status varchar(15),
	occupation varchar(30),
	airline varchar(30)
);

CREATE TABLE IF NOT EXISTS public.airports (
	airport_id bigint,
	name text,
	city text,
	state text,
	type text
);