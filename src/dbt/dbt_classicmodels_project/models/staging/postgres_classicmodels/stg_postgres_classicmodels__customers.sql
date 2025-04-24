with 
	source 
as
(
	select * from {{ source('postgres_classicmodels', 'customers') }}
),
	renamed 
as
(
    select
        customernumber as customer_id,
        customername as customer_name,
        contactlastname as contact_last_name,
        contactfirstname as contact_first_name,
        phone,
        addressline1 as address_line_1,
        addressline2 as address_line_2,
        city,
        state,
        postalcode as postal_code,
        country,
        salesrepemployeenumber as sales_rep_employee_id,
        cast(creditlimit as numeric(10, 2)) as credit_limit_usd
        -- Add _loaded_at or other metadata if available/needed
    from source
)

select * from renamed
