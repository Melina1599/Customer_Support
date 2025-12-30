-- Poblar fact_tickets
INSERT INTO fact_tickets (
	    fk_id_priority, fk_id_category, fk_id_resolution, fk_id_queue, fk_id_context,
	    subject, body, answer, ticket_type, additional_tag
	)
SELECT 
    dp.id_priority, dc.id_category, dr.id_resolution, dq.id_queue, dctx.id_context,
    stm.subject, stm.body, stm.answer, stm.ticket_type, stm.additional_tag
FROM stg_tickets_master stm
LEFT JOIN dim_priority dp ON stm.priority = dp.priority AND stm.priority_level::integer = dp.priority_level
LEFT JOIN dim_category dc 
    ON stm.problem_category = dc.problem_category 
    AND stm.issue_classification = dc.issue_classification
    AND stm.issue_subcategory = dc.issue_subcategory
    AND stm.issue_detail = dc.issue_detail
LEFT JOIN dim_resolution dr 
    ON stm.resolution_category = dr.resolution_category 
    AND stm.resolution_subcategory = dr.resolution_subcategory
    AND stm.resolution_detail = dr.resolution_detail
LEFT JOIN dim_queue dq ON stm.queue = dq.queue AND stm.sector = dq.sector AND stm.specific_sector = dq.specific_sector
LEFT JOIN dim_context dctx ON stm.origin = dctx.origin AND stm.language = dctx.language AND stm.business_type = dctx.business_type;
