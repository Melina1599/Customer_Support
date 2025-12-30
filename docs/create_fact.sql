CREATE TABLE IF NOT EXISTS fact_tickets (
    id_ticket SERIAL PRIMARY KEY,
    subject TEXT,
    body TEXT,
    answer TEXT,
    ticket_type VARCHAR(50),
    additional_tag VARCHAR(100),
    -- Foreign Keys to Dimensions
    fk_id_priority INTEGER REFERENCES dim_priority(id_priority),
    fk_id_category INTEGER REFERENCES dim_category(id_category),
    fk_id_resolution INTEGER REFERENCES dim_resolution(id_resolution),
    fk_id_queue INTEGER REFERENCES dim_queue(id_queue),
    fk_id_context INTEGER REFERENCES dim_context(id_context)
);
