use anyhow::anyhow;
use sqlx::Sqlite;
use sqlx::{
    Execute, QueryBuilder, Result,
    sqlite::{SqliteQueryResult, SqliteRow},
};

use crate::data::init::Storage;

impl Storage {
    pub async fn query(&self, q: &String) -> Result<Vec<SqliteRow>> {
        let mut qb = QueryBuilder::new(q);
        let result = qb.build().fetch_all(&self.pool).await?;
        Ok(result)
    }

    pub fn build_scope_query(&self, user_query: &str) -> anyhow::Result<String> {
        let user_query = user_query.trim_end_matches(';');

        let root_table_name = user_query
            .split_whitespace()
            .collect::<Vec<_>>()
            .windows(2)
            .find(|w| w[0].eq_ignore_ascii_case("from"))
            .map(|w| w[1])
            .ok_or_else(|| anyhow!("No root table detected in user query"))?;

        let root_table = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == root_table_name)
            .ok_or_else(|| anyhow!("Root table '{}' not found in schema", root_table_name))?;

        let pk = &root_table.primary_key;
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("");

        // CTE: Build traversal tree
        qb.push("WITH RECURSIVE scope_tree(\n");
        qb.push("  root_id, path_prefix, pk_value, table_name, depth, visited\n");
        qb.push(") AS (\n");

        // Base case: root table
        qb.push("  SELECT ");
        qb.push(pk);
        qb.push(" AS root_id, '' AS path_prefix, ");
        qb.push(pk);
        qb.push(" AS pk_value, '");
        qb.push(root_table_name);
        qb.push("' AS table_name, 1 AS depth, ',' || ");
        qb.push(pk);
        qb.push(" || ',' AS visited\n");
        qb.push("  FROM (");
        qb.push(user_query);
        qb.push(")\n");

        // Recursive case: follow FKs
        for table in &self.schema.tables {
            for fk in &table.foreign_keys {
                let fk_col = &fk.column;
                let fk_ref_table = &fk.references.table;
                let fk_ref_col = &fk.references.column;

                qb.push("  UNION ALL\n");
                qb.push("  SELECT \n");
                qb.push("    st.root_id,\n");
                qb.push("    CASE WHEN st.path_prefix = '' THEN '");
                qb.push(fk_col);
                qb.push("' ELSE st.path_prefix || '.' || '");
                qb.push(fk_col);
                qb.push("' END AS path_prefix,\n");
                qb.push("    f.");
                qb.push(fk_ref_col);
                qb.push(" AS pk_value,\n");
                qb.push("    '");
                qb.push(fk_ref_table);
                qb.push("' AS table_name,\n");
                qb.push("    st.depth + 1 AS depth,\n");
                qb.push("    st.visited || f.");
                qb.push(fk_ref_col);
                qb.push(" || ',' AS visited\n");
                qb.push("  FROM scope_tree st\n");
                qb.push("  JOIN ");
                qb.push(&table.name);
                qb.push(" src ON src.");
                qb.push(&table.primary_key);
                qb.push(" = st.pk_value\n");
                qb.push("  JOIN ");
                qb.push(fk_ref_table);
                qb.push(" f ON f.");
                qb.push(fk_ref_col);
                qb.push(" = src.");
                qb.push(fk_col);
                qb.push("\n  WHERE st.table_name = '");
                qb.push(&table.name);
                qb.push("'\n    AND instr(st.visited, ',' || f.");
                qb.push(fk_ref_col);
                qb.push(" || ',') = 0\n    AND st.depth < 10\n");
            }
        }

        // Expand: For each row in scope_tree, extract ALL columns
        qb.push("),\nexpanded AS (\n");

        let mut first_table = true;
        for table in &self.schema.tables {
            if !first_table {
                qb.push("  UNION ALL\n");
            }
            first_table = false;

            // Generate one SELECT per column
            let mut first_col = true;
            for col in &table.columns {
                if !first_col {
                    qb.push("  UNION ALL\n");
                }
                first_col = false;

                qb.push("  SELECT \n");
                qb.push("    st.root_id,\n");
                qb.push("    CASE WHEN st.path_prefix = '' THEN '");
                qb.push(&col.name);
                qb.push("' ELSE st.path_prefix || '.' || '");
                qb.push(&col.name);
                qb.push("' END AS path,\n");
                qb.push("    CAST(t.");
                qb.push(&col.name);
                qb.push(" AS TEXT) AS value\n");
                qb.push("  FROM scope_tree st\n");
                qb.push("  JOIN ");
                qb.push(&table.name);
                qb.push(" t ON t.");
                qb.push(&table.primary_key);
                qb.push(" = st.pk_value\n");
                qb.push("  WHERE st.table_name = '");
                qb.push(&table.name);
                qb.push("'\n");
            }
        }

        // Final aggregation
        qb.push(")\n");
        qb.push("SELECT root_id, json_group_object(path, value) AS scope_json\n");
        qb.push("FROM expanded\n");
        qb.push("GROUP BY root_id;");

        Ok(qb.build().sql().to_string())
    }
}
