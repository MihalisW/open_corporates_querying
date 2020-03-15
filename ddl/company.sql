DROP TABLE IF EXISTS public.company;
 
CREATE TABLE public.company (
	id SERIAL PRIMARY KEY,
	name VARCHAR(250),
	company_number VARCHAR(20),
	jurisdiction_code VARCHAR(5),
	incorporation_date DATE,
	dissolution_date DATE,
	company_type VARCHAR(250),
	registry_url VARCHAR(250),
	branch VARCHAR(50),
	branch_status BOOLEAN,
	inactive BOOLEAN,
	current_status VARCHAR(100),
	created_at TIMESTAMP,
	updated_at TIMESTAMP,
	retrieved_at TIMESTAMP,
	opencorporates_url VARCHAR(250),
	previous_names VARCHAR(1000),
	source VARCHAR(500),
	registered_address VARCHAR(250),
	registered_address_in_full VARCHAR(250),
	industry_codes VARCHAR(1000),
	restricted_for_marketing BOOLEAN,
	native_company_number VARCHAR(250),
	ultimate_beneficiary_owners VARCHAR(250),
	extract_tstamp TIMESTAMP 
)