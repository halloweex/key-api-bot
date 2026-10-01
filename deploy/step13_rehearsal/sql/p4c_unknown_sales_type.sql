-- P4c: a sales_type no code knows, for one manager, the only way one can arise
-- without a code change — a trigger on the copy's Silver. The CASE that
-- derives sales_type is closed over the known types, so no data can produce
-- one. `silver.orders.sales_type` carries no CHECK (revision 0006).
-- The rehearsal's copy only (reh-pg); dropped by p4c_drop.sql.
--
-- The manager id is written into the function body rather than read from a
-- setting: the web pool's connections predate any ALTER DATABASE ... SET, and
-- a trigger that raised would fail the derivation instead of mislabelling it.
SELECT format($f$
CREATE OR REPLACE FUNCTION meta.reh_unknown_sales_type() RETURNS trigger
LANGUAGE plpgsql AS $b$
BEGIN
    IF NEW.manager_id = %s THEN
        NEW.sales_type := 'reh_unknown';
    END IF;
    RETURN NEW;
END
$b$
$f$, :'mid'::bigint) \gexec
DROP TRIGGER IF EXISTS reh_unknown_sales_type ON silver.orders;
CREATE TRIGGER reh_unknown_sales_type BEFORE INSERT OR UPDATE ON silver.orders
    FOR EACH ROW EXECUTE FUNCTION meta.reh_unknown_sales_type();
SELECT 'armed';
