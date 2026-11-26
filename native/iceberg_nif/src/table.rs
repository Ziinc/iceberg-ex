//! `Iceberg.Table`: metadata, scans, data file writes and fast appends.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use iceberg::arrow::{schema_to_arrow_schema, RecordBatchPartitionSplitter};
use iceberg::spec::{DataFile, DataFileFormat, TableProperties};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Catalog;
use rustler::{Atom, Env, NifMap, NifResult, ResourceArc, Term};
use uuid::Uuid;

use crate::batches;
use crate::error::Error;
use crate::term::{Bin, Data, Json};
use crate::{filter, spawn};

/// A loaded table and the catalog it commits to.
pub struct TableResource {
    catalog: Arc<dyn Catalog>,
    table: Table,
}

#[rustler::resource_impl]
impl rustler::Resource for TableResource {}

impl TableResource {
    pub fn new(catalog: Arc<dyn Catalog>, table: Table) -> ResourceArc<Self> {
        ResourceArc::new(TableResource { catalog, table })
    }
}

type TableArc = ResourceArc<TableResource>;

/// Written, uncommitted data files.
pub struct DataFiles(Vec<DataFile>);

#[rustler::resource_impl]
impl rustler::Resource for DataFiles {}

#[derive(NifMap)]
struct FileInfo {
    path: String,
    record_count: u64,
    file_size_in_bytes: u64,
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_metadata(t: TableArc) -> NifResult<Json> {
    serde_json::to_value(t.table.metadata())
        .map(Json)
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))
}

#[rustler::nif]
fn table_refresh(env: Env, r: Term, timeout: Option<u64>, t: TableArc) -> Atom {
    spawn(env, r, timeout, async move {
        let table = t.catalog.load_table(t.table.identifier()).await?;
        Ok(TableResource::new(t.catalog.clone(), table))
    })
}

/// The rows `filter` matches, reading only `columns` (the filter's).
#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn table_count(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    t: TableArc,
    columns: Vec<String>,
    filter: Term,
    snapshot_id: Option<i64>,
    case_sensitive: bool,
) -> NifResult<Atom> {
    let filter = filter::decode(filter)?;
    Ok(spawn(env, r, timeout, async move {
        let mut scan = t.table.scan().with_case_sensitive(case_sensitive);
        scan = if columns.is_empty() {
            scan.select_empty()
        } else {
            scan.select(&columns)
        };
        if let Some(predicate) = filter {
            scan = scan.with_filter(predicate);
        }
        if let Some(id) = snapshot_id {
            scan = scan.snapshot_id(id);
        }
        let rows = scan
            .build()?
            .to_arrow()
            .await?
            .try_fold(0u64, |rows, batch| async move {
                Ok(rows + batch.num_rows() as u64)
            })
            .await?;
        Ok(rows)
    }))
}

#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn table_scan(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    t: TableArc,
    select: Option<Vec<String>>,
    filter: Term,
    snapshot_id: Option<i64>,
    batch_size: Option<usize>,
    case_sensitive: bool,
) -> NifResult<Atom> {
    let filter = filter::decode(filter)?;
    Ok(spawn(env, r, timeout, async move {
        let mut scan = t
            .table
            .scan()
            .with_batch_size(batch_size)
            .with_case_sensitive(case_sensitive);
        scan = match &select {
            Some(columns) => scan.select(columns),
            None => scan.select_all(),
        };
        if let Some(predicate) = filter {
            scan = scan.with_filter(predicate);
        }
        if let Some(id) = snapshot_id {
            scan = scan.snapshot_id(id);
        }
        let batches: Vec<RecordBatch> = scan.build()?.to_arrow().await?.try_collect().await?;
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => projected_schema(&t.table, select.as_deref())?,
        };
        Ok(Bin(batches::to_ipc(&schema, &batches)?))
    }))
}

/// The columns an empty scan would have returned.
fn projected_schema(table: &Table, select: Option<&[String]>) -> Result<SchemaRef, Error> {
    let schema = schema_to_arrow_schema(table.metadata().current_schema())?;
    Ok(Arc::new(match select {
        None => schema,
        Some(columns) => {
            let indices = columns
                .iter()
                .map(|c| schema.index_of(c))
                .collect::<Result<Vec<_>, _>>()?;
            schema.project(&indices)?
        }
    }))
}

#[rustler::nif]
fn table_write(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    t: TableArc,
    data: Term,
    prefix: Option<String>,
    props: HashMap<String, String>,
) -> NifResult<Atom> {
    let data = Data::new(data)?;
    Ok(spawn(env, r, timeout, async move {
        let batches = data.batches()?;
        drop(data);
        let prefix = prefix.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let files = write(&t.table, batches, prefix, props).await?;
        let info = files
            .iter()
            .map(|f| FileInfo {
                path: f.file_path().to_string(),
                record_count: f.record_count(),
                file_size_in_bytes: f.file_size_in_bytes(),
            })
            .collect::<Vec<_>>();
        Ok((ResourceArc::new(DataFiles(files)), info))
    }))
}

/// Writes `batches` as Parquet under the table's data location with the
/// table's `write.*` properties, `overrides` taking precedence.
async fn write(
    table: &Table,
    batches: Vec<RecordBatch>,
    prefix: String,
    overrides: HashMap<String, String>,
) -> Result<Vec<DataFile>, Error> {
    let metadata = table.metadata();
    let schema = metadata.current_schema().clone();
    let arrow_schema: SchemaRef = Arc::new(schema_to_arrow_schema(&schema)?);
    let mut props = metadata.properties().clone();
    props.extend(overrides);
    let props = TableProperties::new(&props);

    let parquet = ParquetWriterBuilder::from_table_properties(&props, schema.clone())?;
    let rolling = RollingFileWriterBuilder::new(
        parquet,
        props.write_target_file_size_bytes()?,
        table.file_io().clone(),
        DefaultLocationGenerator::new(metadata)?,
        DefaultFileNameGenerator::new(prefix, None, DataFileFormat::Parquet),
    );
    let builder = DataFileWriterBuilder::new(rolling);
    let spec = metadata.default_partition_spec();

    if spec.is_unpartitioned() {
        let mut writer = builder.build(None).await?;
        for batch in &batches {
            writer
                .write(batches::conform(batch, &arrow_schema)?)
                .await?;
        }
        Ok(writer.close().await?)
    } else {
        let splitter =
            RecordBatchPartitionSplitter::try_new_with_computed_values(schema, spec.clone())?;
        let mut writer = FanoutWriter::new(builder);
        for batch in &batches {
            for (key, part) in splitter.split(&batches::conform(batch, &arrow_schema)?)? {
                writer.write(key, part).await?;
            }
        }
        Ok(writer.close().await?)
    }
}

// Commits to one table from this node are taken one at a time, whichever
// handle they come from: concurrent ones otherwise conflict, and with
// iceberg-rust's few, unjittered retries some fail. Other writers are still
// handled by those retries.
static COMMIT_LOCKS: LazyLock<Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(Default::default);

fn commit_lock(table: Uuid) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = COMMIT_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
    locks.entry(table).or_default().clone()
}

#[rustler::nif]
fn table_fast_append(
    env: Env,
    r: Term,
    timeout: Option<u64>,
    t: TableArc,
    files: Vec<ResourceArc<DataFiles>>,
    snapshot_props: HashMap<String, String>,
    check_duplicate: bool,
) -> Atom {
    spawn(env, r, timeout, async move {
        let files = files
            .iter()
            .flat_map(|f| f.0.iter().cloned())
            .collect::<Vec<_>>();
        let lock = commit_lock(t.table.metadata().uuid());
        let _held = lock.lock().await;
        let tx = Transaction::new(&t.table);
        let tx = tx
            .fast_append()
            .with_check_duplicate(check_duplicate)
            .set_snapshot_properties(snapshot_props)
            .add_data_files(files)
            .apply(tx)?;
        let table = tx.commit(t.catalog.as_ref()).await?;
        Ok(TableResource::new(t.catalog.clone(), table))
    })
}
