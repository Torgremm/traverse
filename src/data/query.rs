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

        // Start CTE
        qb.push("WITH RECURSIVE scope_tree(\n");
        qb.push("  root_id, path, value, table_name, depth, visited\n");
        qb.push(") AS (\n");

        // Base case: root objects
        qb.push("  SELECT ");
        qb.push(pk);
        qb.push(" AS root_id, '");
        qb.push(pk);
        qb.push("' AS path, ");
        qb.push(pk);
        qb.push(" AS value, '");
        qb.push(root_table_name);
        qb.push("' AS table_name, 1 AS depth, ',' || ");
        qb.push(pk);
        qb.push(" || ',' AS visited\n  FROM (");
        qb.push(user_query);
        qb.push(")\n");

        // Recursive case: follow foreign keys
        for table in &self.schema.tables {
            for fk in &table.foreign_keys {
                let fk_col = &fk.column;
                let fk_ref_table = &fk.references.table;
                let fk_ref_col = &fk.references.column;

                qb.push("  UNION ALL\n");
                qb.push("  SELECT \n");
                qb.push("    st.root_id,\n");
                qb.push("    st.path || '.' || '");
                qb.push(fk_col);
                qb.push("' AS path,\n");
                qb.push("    f.");
                qb.push(fk_ref_col);
                qb.push(" AS value,\n");
                qb.push("    '");
                qb.push(fk_ref_table);
                qb.push("' AS table_name,\n");
                qb.push("    st.depth + 1 AS depth,\n");
                qb.push("    st.visited || f.");
                qb.push(fk_ref_col);
                qb.push(" || ',' AS visited\n");
                qb.push("  FROM scope_tree st\n");
                qb.push("  JOIN ");
                qb.push(table.name.as_str());
                qb.push(" src ON src.");
                qb.push(&table.primary_key);
                qb.push(" = st.value\n");
                qb.push("  JOIN ");
                qb.push(fk_ref_table);
                qb.push(" f ON f.");
                qb.push(fk_ref_col);
                qb.push(" = src.");
                qb.push(fk_col);
                qb.push("\n  WHERE st.table_name = '");
                qb.push(&table.name);
                qb.push("'\n");
                qb.push("    AND instr(st.visited, ',' || f.");
                qb.push(fk_ref_col);
                qb.push(" || ',') = 0\n");
                qb.push("    AND st.depth < 10\n");
            }
        }

        // Final SELECT
        qb.push(")\n");
        qb.push("SELECT root_id, json_group_object(path, value) AS scope_json\n");
        qb.push("FROM scope_tree\n");
        qb.push("GROUP BY root_id;");

        Ok(qb.build().sql().to_string())
    }
}
