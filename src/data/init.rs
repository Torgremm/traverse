use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::load::parse_data::{DataFile, Row};
use crate::load::parse_tables::{SchemaConfig, TableConfig};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphEdge {
    pub id: String,
    pub from: String,
    pub to: String,
    pub label: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryRow {
    values: BTreeMap<String, Value>,
}

impl QueryRow {
    pub fn new(values: BTreeMap<String, Value>) -> Self {
        Self { values }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }

    pub fn entries(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.values.iter()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Graph {
    pub(crate) nodes: Vec<GraphNode>,
    pub(crate) edges: Vec<GraphEdge>,
}

pub struct Storage;

#[derive(Debug, Clone)]
pub struct GraphQuery {
    label: String,
    filter: Option<Filter>,
    traverse: Option<Traverse>,
    returns: ReturnSpec,
    set: Option<(String, Value)>,
}

#[derive(Debug, Clone)]
struct Filter {
    field: String,
    op: Operator,
    value: Value,
}

#[derive(Debug, Clone, Copy)]
enum Operator {
    Eq,
    Ne,
}

#[derive(Debug, Clone)]
struct Traverse {
    edge_label: Option<String>,
    depth: usize,
}

#[derive(Debug, Clone)]
enum ReturnSpec {
    All,
    Fields(Vec<String>),
}

impl Storage {
    pub fn graph_dir_for_project(project_path: &Path) -> PathBuf {
        project_path.join("graph")
    }

    pub fn nodes_file_for_project(project_path: &Path) -> PathBuf {
        Self::graph_dir_for_project(project_path).join("nodes.jsonl")
    }

    pub fn edges_file_for_project(project_path: &Path) -> PathBuf {
        Self::graph_dir_for_project(project_path).join("edges.jsonl")
    }

    pub async fn init(schema: SchemaConfig, data: DataFile, path: &Path) -> Result<PathBuf> {
        let graph = Self::graph_from_data(&schema, &data)?;
        Self::write_graph(path, &graph)?;
        Ok(Self::graph_dir_for_project(path))
    }

    pub fn graph_from_data(schema: &SchemaConfig, data: &DataFile) -> Result<Graph> {
        let mut nodes = Vec::new();
        let mut node_ids = HashSet::new();
        let mut target_lookup: HashMap<(String, String, String), String> = HashMap::new();

        for table in &schema.tables {
            let Some(rows) = data.get(&table.name) else {
                continue;
            };

            for row in rows {
                let node = Self::node_from_row(table, row)?;
                if !node_ids.insert(node.id.clone()) {
                    bail!("Duplicate graph node id `{}`", node.id);
                }

                for col in &table.columns {
                    if let Some(value) = node.properties.get(&col.name) {
                        target_lookup.insert(
                            (table.name.clone(), col.name.clone(), value_key(value)),
                            node.id.clone(),
                        );
                    }
                }

                nodes.push(node);
            }
        }

        let mut edges = Vec::new();
        for table in &schema.tables {
            let Some(rows) = data.get(&table.name) else {
                continue;
            };

            for row in rows {
                let from = Self::node_id(
                    table,
                    row.get(&table.primary_key).ok_or_else(|| {
                        anyhow!(
                            "Table `{}` row missing primary key `{}`",
                            table.name,
                            table.primary_key
                        )
                    })?,
                );

                for fk in &table.foreign_keys {
                    let Some(fk_value) = row.get(&fk.column) else {
                        continue;
                    };
                    let target_key = (
                        fk.references.table.clone(),
                        fk.references.column.clone(),
                        value_key(fk_value),
                    );
                    let to = target_lookup.get(&target_key).ok_or_else(|| {
                        anyhow!(
                            "FK violation: `{}`.`{}` = {} does not exist in `{}`.`{}`",
                            table.name,
                            fk.column,
                            fk_value,
                            fk.references.table,
                            fk.references.column
                        )
                    })?;

                    edges.push(GraphEdge {
                        id: format!("{}:{}->{}", from, fk.column, to),
                        from: from.clone(),
                        to: to.clone(),
                        label: fk.column.clone(),
                    });
                }
            }
        }

        nodes.sort_by(|a, b| a.label.cmp(&b.label).then(a.id.cmp(&b.id)));
        edges.sort_by(|a, b| {
            a.label
                .cmp(&b.label)
                .then(a.from.cmp(&b.from))
                .then(a.to.cmp(&b.to))
        });

        Ok(Graph { nodes, edges })
    }

    fn node_from_row(table: &TableConfig, row: &Row) -> Result<GraphNode> {
        let pk = row.get(&table.primary_key).ok_or_else(|| {
            anyhow!(
                "Table `{}` row missing primary key `{}`",
                table.name,
                table.primary_key
            )
        })?;

        let mut properties = BTreeMap::new();
        for col in &table.columns {
            if let Some(value) = row.get(&col.name) {
                properties.insert(col.name.clone(), value.clone());
            }
        }

        Ok(GraphNode {
            id: Self::node_id(table, pk),
            label: table.name.clone(),
            properties,
        })
    }

    fn node_id(table: &TableConfig, pk: &Value) -> String {
        format!("{}:{}", table.name, value_key(pk))
    }

    pub fn read_graph(path: &Path) -> Result<Graph> {
        let nodes = read_jsonl::<GraphNode>(&Self::nodes_file_for_project(path))?;
        let edges = read_jsonl::<GraphEdge>(&Self::edges_file_for_project(path))?;
        Ok(Graph { nodes, edges })
    }

    pub fn write_graph(path: &Path, graph: &Graph) -> Result<()> {
        let graph_dir = Self::graph_dir_for_project(path);
        fs::create_dir_all(&graph_dir)?;

        write_jsonl(&Self::nodes_file_for_project(path), &graph.nodes)?;
        write_jsonl(&Self::edges_file_for_project(path), &graph.edges)?;
        Ok(())
    }

    pub fn type_view(schema: &SchemaConfig, project: &Path) -> Result<Value> {
        let graph = Self::read_graph(project)?;
        Ok(Self::type_view_for_graph(schema, &graph))
    }

    pub fn type_view_for_graph(schema: &SchemaConfig, graph: &Graph) -> Value {
        let mut node_counts: HashMap<&str, usize> = HashMap::new();
        let mut edge_counts: HashMap<&str, usize> = HashMap::new();

        for node in &graph.nodes {
            *node_counts.entry(&node.label).or_default() += 1;
        }
        for edge in &graph.edges {
            *edge_counts.entry(&edge.label).or_default() += 1;
        }

        let types = schema
            .tables
            .iter()
            .map(|table| {
                let columns = table
                    .columns
                    .iter()
                    .map(|column| {
                        json!({
                            "name": column.name,
                            "type": column.col_type,
                            "primary": column.name == table.primary_key,
                        })
                    })
                    .collect::<Vec<_>>();
                let edges = table
                    .foreign_keys
                    .iter()
                    .map(|fk| {
                        json!({
                            "label": fk.column,
                            "from_type": table.name,
                            "from_field": fk.column,
                            "to_type": fk.references.table,
                            "to_field": fk.references.column,
                            "count": edge_counts.get(fk.column.as_str()).copied().unwrap_or(0),
                        })
                    })
                    .collect::<Vec<_>>();

                json!({
                    "name": table.name,
                    "primary_key": table.primary_key,
                    "color": type_color(&table.name),
                    "columns": columns,
                    "edges": edges,
                    "count": node_counts.get(table.name.as_str()).copied().unwrap_or(0),
                })
            })
            .collect::<Vec<_>>();

        json!({ "types": types })
    }

    pub fn table_view(schema: &SchemaConfig, project: &Path, label: &str) -> Result<Value> {
        let graph = Self::read_graph(project)?;
        Self::table_view_for_graph(schema, &graph, label)
    }

    pub fn table_view_for_graph(
        schema: &SchemaConfig,
        graph: &Graph,
        label: &str,
    ) -> Result<Value> {
        let table = schema
            .tables
            .iter()
            .find(|table| table.name == label)
            .ok_or_else(|| anyhow!("Unknown type `{}`", label))?;
        let column_names = table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let rows = graph
            .nodes
            .iter()
            .filter(|node| node.label == label)
            .map(|node| {
                let mut row = serde_json::Map::new();
                row.insert("_id".to_string(), Value::String(node.id.clone()));
                for column in &column_names {
                    row.insert(
                        column.clone(),
                        node.properties.get(column).cloned().unwrap_or(Value::Null),
                    );
                }
                Value::Object(row)
            })
            .collect::<Vec<_>>();

        Ok(json!({
            "type": table.name,
            "primary_key": table.primary_key,
            "color": type_color(&table.name),
            "columns": column_names,
            "rows": rows,
        }))
    }

    pub fn graph_view(schema: &SchemaConfig, project: &Path) -> Result<Value> {
        let graph = Self::read_graph(project)?;
        Ok(Self::graph_view_for_graph(schema, &graph))
    }

    pub fn graph_view_for_graph(schema: &SchemaConfig, graph: &Graph) -> Value {
        let type_colors = schema
            .tables
            .iter()
            .map(|table| (table.name.as_str(), type_color(&table.name)))
            .collect::<HashMap<_, _>>();
        let primary_keys = schema
            .tables
            .iter()
            .map(|table| (table.name.as_str(), table.primary_key.as_str()))
            .collect::<HashMap<_, _>>();

        let nodes = graph
            .nodes
            .iter()
            .map(|node| {
                json!({
                    "id": node.id,
                    "type": node.label,
                    "label": node_label(&primary_keys, node),
                    "color": type_colors
                        .get(node.label.as_str())
                        .cloned()
                        .unwrap_or_else(|| type_color(&node.label)),
                    "properties": node.properties,
                })
            })
            .collect::<Vec<_>>();

        let edges = graph
            .edges
            .iter()
            .map(|edge| {
                json!({
                    "id": edge.id,
                    "from": edge.from,
                    "to": edge.to,
                    "label": edge.label,
                })
            })
            .collect::<Vec<_>>();

        json!({
            "types": Self::type_view_for_graph(schema, graph)["types"].clone(),
            "nodes": nodes,
            "edges": edges,
        })
    }

    pub fn rebuild_graph_from_nodes(schema: &SchemaConfig, graph: &Graph) -> Result<Graph> {
        let mut data = DataFile::new();
        for node in &graph.nodes {
            data.entry(node.label.clone())
                .or_default()
                .push(node.properties.clone().into_iter().collect());
        }
        Self::graph_from_data(schema, &data)
    }

    pub async fn query(q: &str, project: &Path) -> Result<Vec<QueryRow>> {
        let graph = Self::read_graph(project)?;
        Self::query_graph(q, &graph)
    }

    pub fn query_graph(q: &str, graph: &Graph) -> Result<Vec<QueryRow>> {
        let query = GraphQuery::parse(q)?;
        if query.set.is_some() {
            bail!("Use mutating query execution for SET queries");
        }
        Ok(query.execute(&graph))
    }

    pub async fn mutate_with_schema(q: &str, schema: &SchemaConfig, project: &Path) -> Result<u64> {
        let graph = Self::read_graph(project)?;
        let query = GraphQuery::parse(q)?;
        let Some((field, value)) = query.set.clone() else {
            bail!("Mutation queries must include SET <field> = <value>");
        };

        let matched_ids: HashSet<_> = query
            .matching_nodes(&graph)
            .into_iter()
            .map(|node| node.id.clone())
            .collect();

        let mut changed = 0;
        let mut candidate = graph.clone();
        for node in &mut candidate.nodes {
            if !matched_ids.contains(&node.id) {
                continue;
            }
            let table = schema
                .tables
                .iter()
                .find(|table| table.name == node.label)
                .ok_or_else(|| anyhow!("Unknown node type `{}`", node.label))?;
            if field == table.primary_key {
                bail!("Primary key field `{}` cannot be changed", field);
            }
            let column = table
                .columns
                .iter()
                .find(|column| column.name == field)
                .ok_or_else(|| anyhow!("Unknown field `{}` for type `{}`", field, table.name))?;
            validate_property_value(&table.name, &field, &column.col_type, &value)?;
            node.properties.insert(field.clone(), value.clone());
            changed += 1;
        }

        let rebuilt = Self::rebuild_graph_from_nodes(schema, &candidate)?;
        Self::write_graph(project, &rebuilt)?;
        Ok(changed)
    }

    pub async fn scope(
        schema: &SchemaConfig,
        fetch: &str,
        key: &Option<String>,
        project: &Path,
    ) -> Result<Vec<QueryRow>> {
        let graph = Self::read_graph(project)?;
        let query = GraphQuery::parse(fetch)?;
        if query.set.is_some() {
            bail!("Scoped fetch queries cannot mutate graph data");
        }

        let node_by_id: HashMap<_, _> = graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
        let mut edges_by_from: HashMap<&str, Vec<&GraphEdge>> = HashMap::new();
        for edge in &graph.edges {
            edges_by_from.entry(&edge.from).or_default().push(edge);
        }

        let table_by_label: HashMap<_, _> =
            schema.tables.iter().map(|t| (t.name.as_str(), t)).collect();
        let mut rows = Vec::new();
        for root in query.matching_nodes(&graph) {
            let root_table = table_by_label
                .get(root.label.as_str())
                .ok_or_else(|| anyhow!("Root label `{}` not found in schema", root.label))?;
            let root_key = key.as_deref().unwrap_or(&root_table.primary_key);
            let root_id = root
                .properties
                .get(root_key)
                .ok_or_else(|| anyhow!("Root node `{}` has no property `{}`", root.id, root_key))?;
            let root_id = value_key(root_id);

            let mut queue = VecDeque::new();
            let mut visited = HashSet::new();
            queue.push_back((root.id.as_str(), String::new(), 0usize));

            while let Some((node_id, prefix, depth)) = queue.pop_front() {
                if !visited.insert(node_id.to_string()) {
                    continue;
                }
                let Some(node) = node_by_id.get(node_id) else {
                    continue;
                };

                for (field, value) in &node.properties {
                    let path = if prefix.is_empty() {
                        field.clone()
                    } else {
                        format!("{}_{}", prefix, field)
                    };
                    rows.push(QueryRow::new(BTreeMap::from([
                        ("root_id".to_string(), Value::String(root_id.clone())),
                        ("path".to_string(), Value::String(path)),
                        ("value".to_string(), value.clone()),
                    ])));
                }

                if depth >= 10 {
                    continue;
                }

                if let Some(edges) = edges_by_from.get(node_id) {
                    for edge in edges {
                        let next_prefix = if prefix.is_empty() {
                            edge.label.clone()
                        } else {
                            format!("{}_{}", prefix, edge.label)
                        };
                        queue.push_back((edge.to.as_str(), next_prefix, depth + 1));
                    }
                }
            }
        }

        Ok(rows)
    }
}

impl GraphQuery {
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim().trim_end_matches(';');
        let tokens = tokenize(input)?;
        if tokens.len() < 2 || !tokens[0].eq_ignore_ascii_case("MATCH") {
            bail!("Graph queries must start with MATCH <label>");
        }

        let label = tokens[1].clone();
        let mut index = 2;
        let mut filter = None;
        let mut traverse = None;
        let mut returns = ReturnSpec::All;
        let mut set = None;

        while index < tokens.len() {
            match tokens[index].to_ascii_uppercase().as_str() {
                "WHERE" => {
                    if index + 3 >= tokens.len() {
                        bail!("WHERE must be followed by <field> <op> <value>");
                    }
                    filter = Some(Filter {
                        field: tokens[index + 1].clone(),
                        op: Operator::parse(&tokens[index + 2])?,
                        value: parse_value(&tokens[index + 3]),
                    });
                    index += 4;
                }
                "TRAVERSE" => {
                    if index + 1 >= tokens.len() {
                        bail!("TRAVERSE must be followed by an edge label or *");
                    }
                    let edge_label = if tokens[index + 1] == "*" {
                        None
                    } else {
                        Some(tokens[index + 1].clone())
                    };
                    index += 2;
                    let mut depth = 1;
                    if index + 1 < tokens.len() && tokens[index].eq_ignore_ascii_case("DEPTH") {
                        depth = tokens[index + 1].parse::<usize>()?;
                        index += 2;
                    }
                    traverse = Some(Traverse { edge_label, depth });
                }
                "RETURN" => {
                    if index + 1 >= tokens.len() {
                        bail!("RETURN must include * or at least one field");
                    }
                    let fields = tokens[index + 1..]
                        .iter()
                        .take_while(|token| !token.eq_ignore_ascii_case("SET"))
                        .flat_map(|token| token.split(','))
                        .filter(|token| !token.is_empty())
                        .map(|token| token.to_string())
                        .collect::<Vec<_>>();
                    returns = if fields.len() == 1 && fields[0] == "*" {
                        ReturnSpec::All
                    } else {
                        ReturnSpec::Fields(fields)
                    };
                    index += 1;
                    while index < tokens.len() && !tokens[index].eq_ignore_ascii_case("SET") {
                        index += 1;
                    }
                }
                "SET" => {
                    if index + 3 >= tokens.len() || tokens[index + 2] != "=" {
                        bail!("SET must be followed by <field> = <value>");
                    }
                    set = Some((tokens[index + 1].clone(), parse_value(&tokens[index + 3])));
                    index += 4;
                }
                other => bail!("Unexpected token `{}` in graph query", other),
            }
        }

        Ok(Self {
            label,
            filter,
            traverse,
            returns,
            set,
        })
    }

    fn execute(&self, graph: &Graph) -> Vec<QueryRow> {
        let nodes = self.result_nodes(graph);
        nodes
            .into_iter()
            .map(|node| {
                let values = match &self.returns {
                    ReturnSpec::All => node.properties.clone(),
                    ReturnSpec::Fields(fields) => fields
                        .iter()
                        .filter_map(|field| {
                            node.properties
                                .get(field)
                                .map(|value| (field.clone(), value.clone()))
                        })
                        .collect(),
                };
                QueryRow::new(values)
            })
            .collect()
    }

    fn result_nodes<'a>(&self, graph: &'a Graph) -> Vec<&'a GraphNode> {
        let roots = self.matching_nodes(graph);
        let Some(traverse) = &self.traverse else {
            return roots;
        };

        let node_by_id: HashMap<_, _> = graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
        let mut edges_by_from: HashMap<&str, Vec<&GraphEdge>> = HashMap::new();
        for edge in &graph.edges {
            edges_by_from.entry(&edge.from).or_default().push(edge);
        }

        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for root in roots {
            let mut queue = VecDeque::new();
            queue.push_back((root.id.as_str(), 0usize));
            while let Some((node_id, depth)) = queue.pop_front() {
                if depth > 0 {
                    if let Some(node) = node_by_id.get(node_id) {
                        if seen.insert(node.id.as_str()) {
                            out.push(*node);
                        }
                    }
                }
                if depth >= traverse.depth {
                    continue;
                }
                if let Some(edges) = edges_by_from.get(node_id) {
                    for edge in edges {
                        if traverse
                            .edge_label
                            .as_ref()
                            .is_none_or(|label| label == &edge.label)
                        {
                            queue.push_back((edge.to.as_str(), depth + 1));
                        }
                    }
                }
            }
        }
        out
    }

    fn matching_nodes<'a>(&self, graph: &'a Graph) -> Vec<&'a GraphNode> {
        graph
            .nodes
            .iter()
            .filter(|node| node.label == self.label)
            .filter(|node| {
                self.filter
                    .as_ref()
                    .is_none_or(|filter| filter.matches(node))
            })
            .collect()
    }
}

impl Filter {
    fn matches(&self, node: &GraphNode) -> bool {
        let Some(value) = node.properties.get(&self.field) else {
            return false;
        };
        match self.op {
            Operator::Eq => values_equal(value, &self.value),
            Operator::Ne => !values_equal(value, &self.value),
        }
    }
}

impl Operator {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "=" | "==" => Ok(Self::Eq),
            "!=" => Ok(Self::Ne),
            _ => bail!("Unsupported operator `{}`; expected =, ==, or !=", raw),
        }
    }
}

fn read_jsonl<T>(path: &Path) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut values = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        values.push(serde_json::from_str(&line)?);
    }
    Ok(values)
}

fn write_jsonl<T>(path: &Path, values: &[T]) -> Result<()>
where
    T: Serialize,
{
    let tmp_path = path.with_extension("jsonl.tmp");
    let mut file = File::create(&tmp_path)?;
    for value in values {
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
    }
    file.flush()?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn tokenize(input: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '\'' || ch == '"' {
            let quote = ch;
            let mut value = String::new();
            let mut closed = false;
            while let Some(next) = chars.next() {
                if next == quote {
                    closed = true;
                    break;
                }
                value.push(next);
            }
            if !closed {
                bail!("Unterminated quoted value");
            }
            tokens.push(value);
            continue;
        }
        if ch == '=' {
            if chars.peek() == Some(&'=') {
                chars.next();
                tokens.push("==".to_string());
            } else {
                tokens.push("=".to_string());
            }
            continue;
        }
        if ch == '!' && chars.peek() == Some(&'=') {
            chars.next();
            tokens.push("!=".to_string());
            continue;
        }

        let mut value = String::from(ch);
        while let Some(next) = chars.peek() {
            if next.is_whitespace() || *next == '=' || *next == '!' {
                break;
            }
            value.push(*next);
            chars.next();
        }
        tokens.push(value);
    }

    Ok(tokens)
}

fn parse_value(raw: &str) -> Value {
    if raw.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if raw.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    if let Ok(value) = raw.parse::<i64>() {
        return Value::Number(value.into());
    }
    if let Ok(value) = raw.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(value) {
            return Value::Number(number);
        }
    }
    Value::String(raw.to_string())
}

fn value_key(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    left == right || value_key(left) == value_key(right)
}

pub(crate) fn validate_property_value(
    type_name: &str,
    field: &str,
    value_type: &str,
    value: &Value,
) -> Result<()> {
    let ok = match value_type {
        "int" => {
            value.is_i64()
                || value.is_u64()
                || value.as_str().and_then(|s| s.parse::<i64>().ok()).is_some()
        }
        "float" => value.is_f64() || value.as_str().and_then(|s| s.parse::<f64>().ok()).is_some(),
        "text" => value.is_string(),
        "bool" => {
            value.is_boolean()
                || value
                    .as_str()
                    .and_then(|s| s.parse::<bool>().ok())
                    .is_some()
        }
        _ => false,
    };

    if ok {
        Ok(())
    } else {
        bail!(
            "Type `{}` field `{}` expected {}, got {}",
            type_name,
            field,
            value_type,
            value
        )
    }
}

fn type_color(label: &str) -> String {
    const PALETTE: [&str; 12] = [
        "#2563eb", "#dc2626", "#16a34a", "#ca8a04", "#9333ea", "#0891b2", "#ea580c", "#4f46e5",
        "#be123c", "#0d9488", "#7c3aed", "#65a30d",
    ];
    let mut hash = 0usize;
    for byte in label.as_bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(*byte as usize);
    }
    PALETTE[hash % PALETTE.len()].to_string()
}

fn node_label(primary_keys: &HashMap<&str, &str>, node: &GraphNode) -> String {
    primary_keys
        .get(node.label.as_str())
        .and_then(|primary_key| node.properties.get(*primary_key))
        .map(value_key)
        .unwrap_or_else(|| node.id.clone())
}
