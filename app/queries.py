from sqlalchemy import text


def build_common_filters(start_date="", end_date="", region="", tenant="", factor=""):
    clauses = []
    params = {}

    if start_date:
        clauses.append("date >= :start_date")
        params["start_date"] = start_date

    if end_date:
        clauses.append("date <= :end_date")
        params["end_date"] = end_date

    if region:
        clauses.append("region = :region")
        params["region"] = region

    if tenant:
        clauses.append("tenant = :tenant")
        params["tenant"] = tenant

    if factor:
        clauses.append("factor = :factor")
        params["factor"] = factor

    return clauses, params


def get_filter_options(db):
    region_query = text("""
        SELECT DISTINCT region
        FROM employee_health_data
        WHERE region IS NOT NULL AND region <> ''
        ORDER BY region
    """)
    tenant_query = text("""
        SELECT DISTINCT tenant
        FROM employee_health_data
        WHERE tenant IS NOT NULL AND tenant <> ''
        ORDER BY tenant
    """)

    regions = [row[0] for row in db.execute(region_query).fetchall()]
    tenants = [row[0] for row in db.execute(tenant_query).fetchall()]

    return {
        "regions": regions,
        "tenants": tenants,
    }


def get_factor_suffering_overview(db, start_date="", end_date="", region="", tenant="", factor=""):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    base_query = """
        SELECT
            factor,
            COUNT(DISTINCT employee_id) AS employees
        FROM employee_health_data
        WHERE LOWER(status) = 'suffering'
    """

    if clauses:
        base_query += " AND " + " AND ".join(clauses)

    base_query += " GROUP BY factor ORDER BY employees DESC, factor"

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def get_factor_risk_overview(db, start_date="", end_date="", region="", tenant="", factor=""):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    base_query = """
        SELECT
            factor,
            COUNT(DISTINCT employee_id) AS employees
        FROM employee_health_data
        WHERE LOWER(status) = 'at risk'
    """

    if clauses:
        base_query += " AND " + " AND ".join(clauses)

    base_query += " GROUP BY factor ORDER BY employees DESC, factor"

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def get_condition_suffering_overview(db, start_date="", end_date="", region="", tenant="", factor=""):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    base_query = """
        SELECT
            health_condition AS condition,
            COUNT(DISTINCT employee_id) AS employees
        FROM employee_health_data
        WHERE LOWER(status) = 'suffering'
    """

    if clauses:
        base_query += " AND " + " AND ".join(clauses)

    base_query += " GROUP BY health_condition ORDER BY employees DESC, condition"

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def get_condition_risk_overview(db, start_date="", end_date="", region="", tenant="", factor=""):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    base_query = """
        SELECT
            health_condition AS condition,
            COUNT(DISTINCT employee_id) AS employees
        FROM employee_health_data
        WHERE LOWER(status) = 'at risk'
    """

    if clauses:
        base_query += " AND " + " AND ".join(clauses)

    base_query += " GROUP BY health_condition ORDER BY employees DESC, condition"

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def get_factor_condition_suffering(db, start_date="", end_date="", region="", tenant="", factor=""):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    base_query = """
        SELECT
            factor,
            health_condition,
            COUNT(DISTINCT employee_id) AS number_of_employees_suffering
        FROM employee_health_data
        WHERE LOWER(status) = 'suffering'
    """

    if clauses:
        base_query += " AND " + " AND ".join(clauses)

    base_query += " GROUP BY factor, health_condition ORDER BY factor, health_condition"

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def get_factor_severity_suffering(db, start_date="", end_date="", region="", tenant="", factor=""):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    base_query = """
        SELECT
            factor,
            severity,
            COUNT(DISTINCT employee_id) AS number_of_employees_suffering
        FROM employee_health_data
        WHERE LOWER(status) = 'suffering'
    """

    if clauses:
        base_query += " AND " + " AND ".join(clauses)

    base_query += " GROUP BY factor, severity ORDER BY factor, severity"

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def get_condition_factor_improvement_suffering(
    db, start_date="", end_date="", region="", tenant="", factor=""
):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    base_query = """
        SELECT
            health_condition,
            factor,
            ROUND(AVG(improvement_rate)::numeric, 2) AS improvement_rate_percent
        FROM employee_health_data
        WHERE LOWER(status) = 'suffering'
    """

    if clauses:
        base_query += " AND " + " AND ".join(clauses)

    base_query += " GROUP BY health_condition, factor ORDER BY health_condition, factor"

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def get_factor_improvement_suffering(db, start_date="", end_date="", region="", tenant="", factor=""):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    base_query = """
        SELECT
            factor,
            ROUND(AVG(improvement_rate)::numeric, 2) AS improvement_rate_percent
        FROM employee_health_data
        WHERE LOWER(status) = 'suffering'
    """

    if clauses:
        base_query += " AND " + " AND ".join(clauses)

    base_query += " GROUP BY factor ORDER BY improvement_rate_percent DESC, factor"

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def get_summary_cards(db, start_date="", end_date="", region="", tenant="", factor=""):
    clauses, params = build_common_filters(start_date, end_date, region, tenant, factor)

    summary_query = """
        SELECT
            COUNT(DISTINCT employee_id) AS total_employees,
            COUNT(DISTINCT factor) AS factors_tracked,
            COUNT(DISTINCT health_condition) AS health_conditions,
            ROUND(AVG(improvement_rate)::numeric, 2) AS avg_improvement_rate
        FROM employee_health_data
        WHERE 1=1
    """

    if clauses:
        summary_query += " AND " + " AND ".join(clauses)

    summary_row = db.execute(text(summary_query), params).fetchone()
    summary_data = dict(summary_row._mapping)

    monthly_query = """
        WITH monthly_stats AS (
            SELECT
                DATE_TRUNC('month', date) AS month_bucket,
                COUNT(DISTINCT employee_id) AS employees,
                COUNT(DISTINCT factor) AS factors,
                COUNT(DISTINCT health_condition) AS conditions,
                AVG(improvement_rate) AS avg_improvement
            FROM employee_health_data
            WHERE 1=1
    """

    if clauses:
        monthly_query += " AND " + " AND ".join(clauses)

    monthly_query += """
            GROUP BY month_bucket
            ORDER BY month_bucket DESC
            LIMIT 2
        )
        SELECT * FROM monthly_stats
        ORDER BY month_bucket DESC
    """

    monthly_rows = db.execute(text(monthly_query), params).fetchall()

    if len(monthly_rows) >= 2:
        current = dict(monthly_rows[0]._mapping)
        previous = dict(monthly_rows[1]._mapping)
    elif len(monthly_rows) == 1:
        current = dict(monthly_rows[0]._mapping)
        previous = {
            "employees": current["employees"],
            "factors": current["factors"],
            "conditions": current["conditions"],
            "avg_improvement": current["avg_improvement"],
        }
    else:
        current = {
            "employees": 0,
            "factors": 0,
            "conditions": 0,
            "avg_improvement": 0,
        }
        previous = current

    return [
        build_summary_card(
            title="Total Employees",
            value=summary_data["total_employees"],
            current_value=current["employees"],
            previous_value=previous["employees"],
            icon="users",
            inverse_good=False,
        ),
        build_summary_card(
            title="Factors Tracked",
            value=summary_data["factors_tracked"],
            current_value=current["factors"],
            previous_value=previous["factors"],
            icon="activity",
            inverse_good=False,
        ),
        build_summary_card(
            title="Health Conditions",
            value=summary_data["health_conditions"],
            current_value=current["conditions"],
            previous_value=previous["conditions"],
            icon="heart",
            inverse_good=True,
        ),
        build_summary_card(
            title="Avg Improvement Rate",
            value=f'{summary_data["avg_improvement_rate"]}%',
            current_value=current["avg_improvement"] or 0,
            previous_value=previous["avg_improvement"] or 0,
            icon="chart",
            inverse_good=False,
            is_percentage=True,
        ),
    ]


def get_data_explorer_records(
    db,
    search="",
    status="",
    severity="",
    factor="",
    condition_factor="",
    condition="",
    start_date="",
    end_date="",
    region="",
    tenant="",
    limit=100,
):
    base_query = """
        SELECT
            TO_CHAR(date, 'YYYY-MM-DD') AS date,
            employee_id,
            region,
            factor,
            health_condition,
            status,
            severity,
            value,
            unit,
            improvement_rate,
            tenant
        FROM employee_health_data
        WHERE 1=1
    """

    params = {}

    if search:
        base_query += """
            AND (
                CAST(employee_id AS TEXT) ILIKE :search
                OR CAST(region AS TEXT) ILIKE :search
                OR CAST(factor AS TEXT) ILIKE :search
                OR CAST(health_condition AS TEXT) ILIKE :search
                OR CAST(status AS TEXT) ILIKE :search
                OR CAST(severity AS TEXT) ILIKE :search
                OR CAST(unit AS TEXT) ILIKE :search
                OR CAST(tenant AS TEXT) ILIKE :search
            )
        """
        params["search"] = f"%{search}%"

    if status:
        base_query += " AND status = :status"
        params["status"] = status

    if severity:
        base_query += " AND severity = :severity"
        params["severity"] = severity

    if factor:
        base_query += " AND factor = :factor"
        params["factor"] = factor

    if condition_factor:
        base_query += " AND factor = :condition_factor"
        params["condition_factor"] = condition_factor

    if condition:
        base_query += " AND health_condition = :condition"
        params["condition"] = condition

    if start_date:
        base_query += " AND date >= :start_date"
        params["start_date"] = start_date

    if end_date:
        base_query += " AND date <= :end_date"
        params["end_date"] = end_date

    if region:
        base_query += " AND region = :region"
        params["region"] = region

    if tenant:
        base_query += " AND tenant = :tenant"
        params["tenant"] = tenant

    base_query += """
        ORDER BY date DESC, employee_id
        LIMIT :limit
    """
    params["limit"] = limit

    result = db.execute(text(base_query), params)
    return [dict(row._mapping) for row in result]


def build_summary_card(
    title,
    value,
    current_value,
    previous_value,
    icon,
    inverse_good=False,
    is_percentage=False,
):
    current_value = current_value or 0
    previous_value = previous_value or 0

    if current_value > previous_value:
        direction = "up"
    elif current_value < previous_value:
        direction = "down"
    else:
        direction = "flat"

    if direction == "flat":
        trend = "flat"
        change = "0%"
    else:
        if previous_value == 0:
            diff_percent = 100.0
        else:
            diff_percent = abs((current_value - previous_value) / previous_value * 100)

        formatted_diff = round(diff_percent, 1)
        formatted_change = f"+{formatted_diff}%" if direction == "up" else f"-{formatted_diff}%"

        if inverse_good:
            trend = "down" if direction == "down" else "up"
        else:
            trend = direction

        change = formatted_change

    if is_percentage and isinstance(value, str) and value.endswith(".0%"):
        value = value.replace(".0%", "%")

    return {
        "title": title,
        "value": value,
        "trend": trend,
        "change": change,
        "icon": icon,
    }