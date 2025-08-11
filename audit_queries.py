# audit_queries.py - SQL Queries for Warehouse Audit System

class AuditQueries:
    """Collection of SQL queries for audit system"""
    
    # ============== SESSION QUERIES ==============
    
    INSERT_SESSION = """
    INSERT INTO audit_sessions (
        session_code, session_name, warehouse_id,
        planned_start_date, planned_end_date, notes,
        status, created_by_user_id, created_date
    ) VALUES (
        :session_code, :session_name, :warehouse_id,
        :planned_start_date, :planned_end_date, :notes,
        'draft', :created_by_user_id, NOW()
    )
    """
    
    START_SESSION = """
    UPDATE audit_sessions 
    SET 
        status = 'in_progress',
        actual_start_date = NOW(),
        modified_by_user_id = :user_id,
        modified_date = NOW()
    WHERE id = :session_id
    AND status = 'draft'
    AND delete_flag = 0
    """
    
    COMPLETE_SESSION = """
    UPDATE audit_sessions 
    SET 
        status = 'completed',
        actual_end_date = NOW(),
        completed_by_user_id = :user_id,
        completed_date = NOW(),
        modified_by_user_id = :user_id,
        modified_date = NOW()
    WHERE id = :session_id
    AND status = 'in_progress'
    AND delete_flag = 0
    """
    
    GET_SESSIONS_BY_STATUS = """
    SELECT 
        ass.*,
        wh.name as warehouse_name,
        u_created.username as created_by_username,
        CONCAT(e_created.first_name, ' ', e_created.last_name) as created_by_name,
        u_completed.username as completed_by_username,
        CONCAT(e_completed.first_name, ' ', e_completed.last_name) as completed_by_name
    FROM audit_sessions ass
    LEFT JOIN warehouses wh ON ass.warehouse_id = wh.id
    LEFT JOIN users u_created ON ass.created_by_user_id = u_created.id
    LEFT JOIN employees e_created ON u_created.employee_id = e_created.id
    LEFT JOIN users u_completed ON ass.completed_by_user_id = u_completed.id
    LEFT JOIN employees e_completed ON u_completed.employee_id = e_completed.id
    WHERE ass.status = :status
    AND ass.delete_flag = 0
    ORDER BY ass.created_date DESC
    LIMIT :limit
    """
    
    GET_ALL_SESSIONS = """
    SELECT 
        ass.*,
        wh.name as warehouse_name,
        u_created.username as created_by_username,
        CONCAT(e_created.first_name, ' ', e_created.last_name) as created_by_name
    FROM audit_sessions ass
    LEFT JOIN warehouses wh ON ass.warehouse_id = wh.id
    LEFT JOIN users u_created ON ass.created_by_user_id = u_created.id
    LEFT JOIN employees e_created ON u_created.employee_id = e_created.id
    WHERE ass.delete_flag = 0
    ORDER BY ass.created_date DESC
    LIMIT :limit
    """
    
    GET_SESSION_INFO = """
    SELECT 
        ass.*,
        wh.name as warehouse_name,
        wh.company_id as warehouse_company_id,
        u_created.username as created_by_username,
        CONCAT(e_created.first_name, ' ', e_created.last_name) as created_by_name
    FROM audit_sessions ass
    LEFT JOIN warehouses wh ON ass.warehouse_id = wh.id
    LEFT JOIN users u_created ON ass.created_by_user_id = u_created.id
    LEFT JOIN employees e_created ON u_created.employee_id = e_created.id
    WHERE ass.id = :session_id
    AND ass.delete_flag = 0
    """
    
    GET_SESSION_PROGRESS = """
    SELECT 
        COUNT(at.id) as total_transactions,
        SUM(CASE WHEN at.status = 'completed' THEN 1 ELSE 0 END) as completed_transactions,
        CASE 
            WHEN COUNT(at.id) > 0 THEN 
                ROUND((SUM(CASE WHEN at.status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(at.id)), 2)
            ELSE 0 
        END as completion_rate,
        COALESCE(SUM(at.total_items_counted), 0) as total_items,
        COALESCE(SUM(at.total_value_counted), 0) as total_value
    FROM audit_transactions at
    WHERE at.session_id = :session_id
    AND at.delete_flag = 0
    """
    
    # ============== TRANSACTION QUERIES ==============
    
    INSERT_TRANSACTION = """
    INSERT INTO audit_transactions (
        session_id, transaction_code, transaction_name,
        assigned_zones, assigned_categories, notes,
        status, created_by_user_id, created_date
    ) VALUES (
        :session_id, :transaction_code, :transaction_name,
        :assigned_zones, :assigned_categories, :notes,
        'draft', :created_by_user_id, NOW()
    )
    """
    
    SUBMIT_TRANSACTION = """
    UPDATE audit_transactions 
    SET 
        status = 'completed',
        submitted_date = :submit_time,
        submitted_by_user_id = :user_id,
        modified_by_user_id = :user_id,
        modified_date = NOW()
    WHERE id = :transaction_id
    """
    
    GET_USER_TRANSACTIONS = """
    SELECT 
        at.*,
        ass.session_name,
        ass.warehouse_id,
        wh.name as warehouse_name
    FROM audit_transactions at
    JOIN audit_sessions ass ON at.session_id = ass.id
    LEFT JOIN warehouses wh ON ass.warehouse_id = wh.id
    WHERE at.session_id = :session_id
    AND at.created_by_user_id = :user_id
    AND at.delete_flag = 0
    ORDER BY at.created_date DESC
    """
    
    GET_USER_TRANSACTIONS_BY_STATUS = """
    SELECT 
        at.*,
        ass.session_name,
        ass.warehouse_id,
        wh.name as warehouse_name
    FROM audit_transactions at
    JOIN audit_sessions ass ON at.session_id = ass.id
    LEFT JOIN warehouses wh ON ass.warehouse_id = wh.id
    WHERE at.session_id = :session_id
    AND at.created_by_user_id = :user_id
    AND at.status = :status
    AND at.delete_flag = 0
    ORDER BY at.created_date DESC
    """
    
    GET_TRANSACTION_INFO = """
    SELECT 
        at.*,
        ass.session_name,
        ass.warehouse_id,
        wh.name as warehouse_name,
        u_created.username as created_by_username
    FROM audit_transactions at
    JOIN audit_sessions ass ON at.session_id = ass.id
    LEFT JOIN warehouses wh ON ass.warehouse_id = wh.id
    LEFT JOIN users u_created ON at.created_by_user_id = u_created.id
    WHERE at.id = :transaction_id
    AND at.delete_flag = 0
    """
    
    GET_TRANSACTION_PROGRESS = """
    SELECT 
        COUNT(acd.id) as items_counted,
        COALESCE(SUM(acd.actual_quantity * acd.system_value_usd / NULLIF(acd.system_quantity, 0)), 0) as total_value
    FROM audit_count_details acd
    WHERE acd.transaction_id = :transaction_id
    AND acd.delete_flag = 0
    """
    
    UPDATE_TRANSACTION_COUNTS = """
    UPDATE audit_transactions 
    SET 
        total_items_counted = (
            SELECT COUNT(*) 
            FROM audit_count_details 
            WHERE transaction_id = :transaction_id 
            AND delete_flag = 0
        ),
        total_value_counted = (
            SELECT COALESCE(SUM(actual_quantity * system_value_usd / NULLIF(system_quantity, 0)), 0)
            FROM audit_count_details 
            WHERE transaction_id = :transaction_id 
            AND delete_flag = 0
        ),
        modified_date = NOW()
    WHERE id = :transaction_id
    """
    
    # ============== COUNT DETAIL QUERIES ==============
    
    CHECK_EXISTING_COUNT = """
    SELECT id, actual_quantity, actual_notes
    FROM audit_count_details
    WHERE transaction_id = :transaction_id
    AND (
        (product_id = :product_id AND :product_id IS NOT NULL)
        OR (:product_id IS NULL AND is_new_item = :is_new_item)
    )
    AND batch_no = :batch_no
    AND delete_flag = 0
    """
    
    INSERT_COUNT_DETAIL = """
    INSERT INTO audit_count_details (
        transaction_id, product_id, batch_no, expired_date,
        zone_name, rack_name, bin_name, location_notes,
        system_quantity, system_value_usd,
        actual_quantity, actual_notes,
        is_new_item, counted_date, created_by_user_id, created_date
    ) VALUES (
        :transaction_id, :product_id, :batch_no, :expired_date,
        :zone_name, :rack_name, :bin_name, :location_notes,
        :system_quantity, :system_value_usd,
        :actual_quantity, :actual_notes,
        :is_new_item, :counted_date, :created_by_user_id, NOW()
    )
    """
    
    UPDATE_COUNT_DETAIL = """
    UPDATE audit_count_details
    SET 
        actual_quantity = :actual_quantity,
        actual_notes = :actual_notes,
        zone_name = :zone_name,
        rack_name = :rack_name,
        bin_name = :bin_name,
        location_notes = :location_notes,
        modified_by_user_id = :modified_by_user_id,
        modified_date = :modified_date,
        counted_date = NOW()
    WHERE id = :count_id
    """
    
    GET_RECENT_COUNTS = """
    SELECT 
        acd.*,
        p.name as product_name,
        p.pt_code,
        b.brand_name
    FROM audit_count_details acd
    LEFT JOIN products p ON acd.product_id = p.id
    LEFT JOIN brands b ON p.brand_id = b.id
    WHERE acd.transaction_id = :transaction_id
    AND acd.delete_flag = 0
    ORDER BY acd.counted_date DESC
    LIMIT :limit
    """
    
    # ============== PRODUCT AND INVENTORY QUERIES ==============
    
    GET_WAREHOUSES = """
    SELECT 
        id, 
        name,
        company_id,
        address as location,
        CASE WHEN delete_flag = 0 THEN 1 ELSE 0 END as is_active
    FROM warehouses
    WHERE delete_flag = 0
    ORDER BY name
    """
    
    GET_WAREHOUSE_DETAIL = """
    SELECT 
        wh.id,
        wh.name,
        wh.address,
        wh.zipcode,
        c.english_name as company_name,
        c.local_name as company_local_name,
        co.name as country_name,
        s.name as state_province,
        CONCAT(e.first_name, ' ', e.last_name) as manager_name,
        e.email as manager_email,
        wh.created_date,
        wh.modified_date
    FROM warehouses wh
    LEFT JOIN companies c ON wh.company_id = c.id
    LEFT JOIN countries co ON wh.country_id = co.id
    LEFT JOIN states s ON wh.state_id = s.id
    LEFT JOIN employees e ON wh.manager_id = e.id
    WHERE wh.id = :warehouse_id
    AND wh.delete_flag = 0
    """
    
    GET_WAREHOUSE_BASIC = """
    SELECT 
        id,
        name,
        address,
        zipcode,
        company_id,
        manager_id,
        country_id,
        state_id,
        created_date,
        modified_date
    FROM warehouses
    WHERE id = :warehouse_id
    AND delete_flag = 0
    """
    
    SEARCH_PRODUCTS = """
    SELECT DISTINCT 
        idv.product_id,
        idv.product_name,
        idv.pt_code,
        idv.legacy_code,
        idv.brand,
        idv.package_size,
        idv.standard_uom,
        idv.warehouse_name
    FROM inventory_detailed_view idv
    WHERE idv.warehouse_id = :warehouse_id
    AND (
        idv.pt_code LIKE :search_term 
        OR idv.legacy_code LIKE :search_term 
        OR idv.product_name LIKE :search_term
        OR idv.brand LIKE :search_term
    )
    AND idv.remaining_quantity > 0
    ORDER BY idv.product_name
    LIMIT 20
    """
    
    GET_WAREHOUSE_PRODUCTS = """
    SELECT DISTINCT 
        idv.product_id,
        idv.product_name,
        idv.pt_code,
        idv.legacy_code,
        idv.brand,
        idv.package_size,
        idv.standard_uom,
        COUNT(*) as total_batches,
        SUM(idv.remaining_quantity) as total_quantity
    FROM inventory_detailed_view idv
    WHERE idv.warehouse_id = :warehouse_id
    AND idv.remaining_quantity > 0
    GROUP BY idv.product_id, idv.product_name, idv.pt_code, idv.legacy_code, idv.brand, idv.package_size, idv.standard_uom
    ORDER BY idv.brand, idv.product_name
    """
    
    GET_WAREHOUSE_BRANDS = """
    SELECT DISTINCT idv.brand
    FROM inventory_detailed_view idv
    WHERE idv.warehouse_id = :warehouse_id
    AND idv.remaining_quantity > 0
    AND idv.brand IS NOT NULL
    AND idv.brand != ''
    ORDER BY idv.brand
    """
    
    SEARCH_PRODUCTS_WITH_FILTERS = """
    SELECT DISTINCT 
        idv.product_id,
        idv.product_name,
        idv.pt_code,
        idv.legacy_code,
        idv.brand,
        idv.package_size,
        idv.standard_uom,
        COUNT(*) as total_batches,
        SUM(idv.remaining_quantity) as total_quantity
    FROM inventory_detailed_view idv
    WHERE idv.warehouse_id = :warehouse_id
    AND idv.remaining_quantity > 0
    AND (:brand_filter = '' OR idv.brand = :brand_filter)
    AND (
        :search_term = '' OR
        idv.pt_code LIKE :search_term OR 
        idv.legacy_code LIKE :search_term OR 
        idv.product_name LIKE :search_term
    )
    GROUP BY idv.product_id, idv.product_name, idv.pt_code, idv.legacy_code, idv.brand, idv.package_size, idv.standard_uom
    ORDER BY idv.brand, idv.product_name
    LIMIT 100
    """
    
    GET_PRODUCT_SYSTEM_INVENTORY = """
    SELECT 
        idv.product_id,
        idv.product_name,
        idv.batch_number as batch_no,
        idv.expiry_date as expired_date,
        idv.remaining_quantity as quantity,
        idv.inventory_value_usd as value_usd,
        idv.location,
        SUBSTRING_INDEX(idv.location, '-', 1) as zone_name,
        CASE 
            WHEN LOCATE('-', idv.location) > 0 THEN
                SUBSTRING_INDEX(SUBSTRING_INDEX(idv.location, '-', 2), '-', -1)
            ELSE ''
        END as rack_name,
        CASE 
            WHEN LENGTH(idv.location) - LENGTH(REPLACE(idv.location, '-', '')) >= 2 THEN
                SUBSTRING_INDEX(idv.location, '-', -1)
            ELSE ''
        END as bin_name
    FROM inventory_detailed_view idv
    WHERE idv.warehouse_id = :warehouse_id
    AND idv.product_id = :product_id
    AND idv.remaining_quantity > 0
    ORDER BY idv.expiry_date ASC
    LIMIT 1
    """
    
    # ============== NEW BATCH DETAILS QUERY ==============
    GET_PRODUCT_BATCH_DETAILS = """
    SELECT 
        idv.batch_number as batch_no,
        idv.expiry_date as expired_date,
        idv.remaining_quantity as quantity,
        idv.location,
        idv.inventory_value_usd as value_usd,
        SUBSTRING_INDEX(idv.location, '-', 1) as zone_name,
        CASE 
            WHEN LOCATE('-', idv.location) > 0 THEN
                SUBSTRING_INDEX(SUBSTRING_INDEX(idv.location, '-', 2), '-', -1)
            ELSE ''
        END as rack_name,
        CASE 
            WHEN LENGTH(idv.location) - LENGTH(REPLACE(idv.location, '-', '')) >= 2 THEN
                SUBSTRING_INDEX(idv.location, '-', -1)
            ELSE ''
        END as bin_name
    FROM inventory_detailed_view idv
    WHERE idv.warehouse_id = :warehouse_id
    AND idv.product_id = :product_id
    AND idv.remaining_quantity > 0
    ORDER BY idv.expiry_date ASC
    """
    
    # ============== DASHBOARD AND STATS QUERIES ==============
    
    GET_DASHBOARD_STATS = """
    SELECT 
        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as active_sessions,
        SUM(CASE WHEN status = 'draft' THEN 1 ELSE 0 END) as draft_sessions,
        SUM(CASE WHEN status = 'completed' AND DATE(completed_date) = CURDATE() THEN 1 ELSE 0 END) as completed_today,
        (
            SELECT COUNT(DISTINCT created_by_user_id)
            FROM audit_transactions 
            WHERE DATE(created_date) = CURDATE()
            AND delete_flag = 0
        ) as active_users
    FROM audit_sessions
    WHERE delete_flag = 0
    """
    
    GET_DAILY_STATS = """
    SELECT 
        DATE(created_date) as audit_date,
        COUNT(*) as sessions_created,
        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as sessions_started,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as sessions_completed
    FROM audit_sessions
    WHERE delete_flag = 0
    AND created_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
    GROUP BY DATE(created_date)
    ORDER BY audit_date DESC
    """
    
    GET_USER_ACTIVITY_STATS = """
    SELECT 
        u.username,
        CONCAT(e.first_name, ' ', e.last_name) as full_name,
        COUNT(DISTINCT at.id) as transactions_created,
        COUNT(DISTINCT acd.id) as items_counted,
        COALESCE(SUM(acd.actual_quantity), 0) as total_quantity_counted,
        MAX(acd.counted_date) as last_activity
    FROM users u
    LEFT JOIN employees e ON u.employee_id = e.id
    LEFT JOIN audit_transactions at ON u.id = at.created_by_user_id AND at.delete_flag = 0
    LEFT JOIN audit_count_details acd ON at.id = acd.transaction_id AND acd.delete_flag = 0
    WHERE u.is_active = 1
    AND u.delete_flag = 0
    AND (at.created_date >= DATE_SUB(NOW(), INTERVAL 30 DAY) OR at.created_date IS NULL)
    GROUP BY u.id, u.username, e.first_name, e.last_name
    HAVING transactions_created > 0 OR items_counted > 0
    ORDER BY last_activity DESC
    LIMIT 20
    """
    
    # ============== REPORTING QUERIES ==============
    
    GET_SESSION_REPORT_DATA = """
    SELECT 
        ass.session_code,
        ass.session_name,
        wh.name as warehouse_name,
        at.transaction_code,
        at.transaction_name,
        at.assigned_zones,
        at.assigned_categories,
        u.username as counted_by,
        CONCAT(e.first_name, ' ', e.last_name) as counter_name,
        acd.product_id,
        COALESCE(p.name, 'NEW ITEM') as product_name,
        COALESCE(p.pt_code, 'N/A') as pt_code,
        COALESCE(b.brand_name, 'N/A') as brand,
        acd.batch_no,
        acd.expired_date,
        acd.zone_name,
        acd.rack_name,
        acd.bin_name,
        acd.location_notes,
        acd.system_quantity,
        acd.system_value_usd,
        acd.actual_quantity,
        acd.actual_notes,
        (acd.actual_quantity - acd.system_quantity) as variance_quantity,
        CASE 
            WHEN acd.system_quantity > 0 THEN
                ROUND((acd.actual_quantity * acd.system_value_usd / acd.system_quantity), 2)
            ELSE 0
        END as actual_value_usd,
        CASE 
            WHEN acd.system_quantity > 0 THEN
                ROUND(((acd.actual_quantity * acd.system_value_usd / acd.system_quantity) - acd.system_value_usd), 2)
            ELSE 0
        END as variance_value_usd,
        CASE 
            WHEN acd.system_quantity > 0 THEN
                ROUND(((acd.actual_quantity - acd.system_quantity) / acd.system_quantity * 100), 2)
            ELSE 0
        END as variance_percentage,
        acd.is_new_item,
        acd.counted_date,
        at.status as transaction_status,
        ass.status as session_status
    FROM audit_sessions ass
    JOIN audit_transactions at ON ass.id = at.session_id
    JOIN audit_count_details acd ON at.id = acd.transaction_id
    LEFT JOIN warehouses wh ON ass.warehouse_id = wh.id
    LEFT JOIN users u ON acd.created_by_user_id = u.id
    LEFT JOIN employees e ON u.employee_id = e.id
    LEFT JOIN products p ON acd.product_id = p.id
    LEFT JOIN brands b ON p.brand_id = b.id
    WHERE ass.id = :session_id
    AND ass.delete_flag = 0
    AND at.delete_flag = 0
    AND acd.delete_flag = 0
    ORDER BY at.transaction_code, p.name, acd.counted_date
    """
    
    GET_VARIANCE_ANALYSIS = """
    SELECT 
        p.name as product_name,
        p.pt_code,
        acd.batch_no,
        acd.system_quantity,
        acd.actual_quantity,
        (acd.actual_quantity - acd.system_quantity) as variance_quantity,
        CASE 
            WHEN acd.system_quantity > 0 THEN
                ROUND((acd.actual_quantity * acd.system_value_usd / acd.system_quantity), 2)
            ELSE 0
        END as actual_value_usd,
        CASE 
            WHEN acd.system_quantity > 0 THEN
                ROUND(((acd.actual_quantity * acd.system_value_usd / acd.system_quantity) - acd.system_value_usd), 2)
            ELSE 0
        END as variance_value,
        CASE 
            WHEN acd.system_quantity > 0 THEN
                ROUND(((acd.actual_quantity - acd.system_quantity) / acd.system_quantity * 100), 2)
            ELSE 0
        END as variance_percentage
    FROM audit_count_details acd
    JOIN audit_transactions at ON acd.transaction_id = at.id
    LEFT JOIN products p ON acd.product_id = p.id
    WHERE at.session_id = :session_id
    AND acd.delete_flag = 0
    AND at.delete_flag = 0
    AND (acd.actual_quantity - acd.system_quantity) != 0
    ORDER BY ABS(acd.actual_quantity - acd.system_quantity) DESC
    """
    
    # ============== LOAD SYSTEM INVENTORY QUERY ==============
    
    LOAD_SYSTEM_INVENTORY = """
    INSERT INTO audit_count_details (
        transaction_id, product_id, batch_no, expired_date,
        zone_name, rack_name, bin_name,
        system_quantity, system_value_usd,
        actual_quantity, 
        is_new_item, created_by_user_id, created_date
    )
    SELECT 
        :transaction_id as transaction_id,
        idv.product_id,
        idv.batch_number as batch_no,
        idv.expiry_date as expired_date,
        SUBSTRING_INDEX(idv.location, '-', 1) as zone_name,
        CASE 
            WHEN LOCATE('-', idv.location) > 0 THEN
                SUBSTRING_INDEX(SUBSTRING_INDEX(idv.location, '-', 2), '-', -1)
            ELSE ''
        END as rack_name,
        CASE 
            WHEN LENGTH(idv.location) - LENGTH(REPLACE(idv.location, '-', '')) >= 2 THEN
                SUBSTRING_INDEX(idv.location, '-', -1)
            ELSE ''
        END as bin_name,
        idv.remaining_quantity as system_quantity,
        idv.inventory_value_usd as system_value_usd,
        0 as actual_quantity,
        0 as is_new_item,
        :created_by_user_id,
        NOW()
    FROM inventory_detailed_view idv
    WHERE idv.warehouse_id = :warehouse_id
    AND idv.remaining_quantity > 0
    """
    
    # ============== UTILITY QUERIES ==============
    
    CHECK_SESSION_EXISTS = """
    SELECT id FROM audit_sessions 
    WHERE session_code = :session_code 
    AND delete_flag = 0
    """
    
    CHECK_TRANSACTION_EXISTS = """
    SELECT id FROM audit_transactions 
    WHERE transaction_code = :transaction_code 
    AND delete_flag = 0
    """
    
    GET_SESSION_TRANSACTIONS_COUNT = """
    SELECT 
        COUNT(*) as total_transactions,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_transactions
    FROM audit_transactions
    WHERE session_id = :session_id
    AND delete_flag = 0
    """
    
    GET_TRANSACTION_ITEMS_COUNT = """
    SELECT COUNT(*) as items_count
    FROM audit_count_details
    WHERE transaction_id = :transaction_id
    AND delete_flag = 0
    """
    
    # ============== CLEANUP QUERIES ==============
    
    SOFT_DELETE_SESSION = """
    UPDATE audit_sessions 
    SET 
        delete_flag = 1,
        modified_by_user_id = :user_id,
        modified_date = NOW()
    WHERE id = :session_id
    """
    
    SOFT_DELETE_TRANSACTION = """
    UPDATE audit_transactions 
    SET 
        delete_flag = 1,
        modified_by_user_id = :user_id,
        modified_date = NOW()
    WHERE id = :transaction_id
    """
    
    SOFT_DELETE_COUNT_DETAIL = """
    UPDATE audit_count_details 
    SET 
        delete_flag = 1,
        modified_by_user_id = :user_id,
        modified_date = NOW()
    WHERE id = :count_id
    """