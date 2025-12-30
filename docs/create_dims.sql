-- 1. Dimensión de Prioridad: priority, priority_level
CREATE TABLE IF NOT EXISTS dim_priority (
    id_priority SERIAL PRIMARY KEY,
    priority VARCHAR(50),      
    priority_level INTEGER,
    UNIQUE (priority, priority_level)
);

-- 2. Dimensión de Tags: problem_category, issue_classification, issue_subcategory, issue_detail
CREATE TABLE IF NOT EXISTS dim_category (
    id_category SERIAL PRIMARY KEY,
    problem_category VARCHAR(100),
    issue_classification VARCHAR(100),
    issue_subcategory VARCHAR(100),
    issue_detail TEXT,
    UNIQUE (problem_category, issue_classification, issue_subcategory, issue_detail)
);

-- 3. Dimensión de Resolution: resolution_category, resolution_subcategory, resolution_detail
CREATE TABLE IF NOT EXISTS dim_resolution (
    id_resolution SERIAL PRIMARY KEY,
    resolution_category VARCHAR(100),  
    resolution_subcategory VARCHAR(100), 
    resolution_detail TEXT,
    UNIQUE (resolution_category, resolution_subcategory, resolution_detail)
);

-- 4. Dimensión de Queue: queue, sector, specific_sector
CREATE TABLE IF NOT EXISTS dim_queue (
    id_queue SERIAL PRIMARY KEY,
    queue VARCHAR(100),              
    sector VARCHAR(100),           
    specific_sector VARCHAR(100),
    UNIQUE (queue, sector, specific_sector)
);

-- 5. Dimensión de Contexto y Origen: origin, language, business_type
CREATE TABLE IF NOT EXISTS dim_context (
    id_context SERIAL PRIMARY KEY,
    origin VARCHAR(50),          
    language VARCHAR(20),           
    business_type VARCHAR(50),
    UNIQUE (origin, language, business_type)
);
