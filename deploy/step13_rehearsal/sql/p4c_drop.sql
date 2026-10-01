-- P4c, undone: the trigger and its function off the copy's Silver.
DROP TRIGGER IF EXISTS reh_unknown_sales_type ON silver.orders;
DROP FUNCTION IF EXISTS meta.reh_unknown_sales_type();
SELECT 'dropped';
