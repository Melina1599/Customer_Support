-- Poblar dim_priority
INSERT INTO dim_priority (priority, priority_level)
SELECT DISTINCT priority, priority_level::integer
FROM stg_tickets_master
WHERE priority IS NOT NULL;

-- Poblar dim_category
INSERT INTO dim_category (problem_category, issue_classification, issue_subcategory, issue_detail)
SELECT DISTINCT 
    problem_category, 
    issue_classification, 
    issue_subcategory, 
    issue_detail
FROM stg_tickets_master;

-- Poblar dim_resolution
INSERT INTO dim_resolution (resolution_category, resolution_subcategory, resolution_detail)
SELECT DISTINCT 
    resolution_category, 
    resolution_subcategory, 
    resolution_detail
FROM stg_tickets_master;

-- Poblar dim_queue
INSERT INTO dim_queue (queue, sector, specific_sector)
SELECT DISTINCT 
    queue, 
    sector, 
    specific_sector
FROM stg_tickets_master;

-- Poblar dim_context
INSERT INTO dim_context (origin, language, business_type)
SELECT DISTINCT 
    origin, 
    language, 
    business_type
FROM stg_tickets_master;