//! `Iceberg.Table`: metadata, scans, data file writes and fast appends.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFile, DataFileFormat, TableProperties};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Catalog;
use rustler::{Atom, Binary, Env, NifMap, NifResult, ResourceArc, Term};

use crate::batches;
use crate::error::Error;
use crate::term::{Bin, Json, SavedBinary};
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
    ipc: Binary,
    prefix: Option<String>,
    props: HashMap<String, String>,
) -> Atom {
    let ipc = SavedBinary::new(ipc);
    spawn(env, r, timeout, async move {
        let batches = ipc.with(batches::from_ipc)?;
        drop(ipc);
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
    })
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
    if !metadata.default_partition_spec().is_unpartitioned() {
        return Err(Error::new(
            "feature_unsupported",
            "writes to partitioned tables are not supported yet",
        ));
    }
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
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    for batch in &batches {
        writer
            .write(batches::conform(batch, &arrow_schema)?)
            .await?;
    }
    Ok(writer.close().await?)
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

#[cfg(test)]
mod tests {
    use iceberg::memory::MemoryCatalogBuilder;
    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
    };
    use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation};
    use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

    use super::*;

    #[test]
    fn writes_to_partitioned_tables_are_refused() {
        crate::RUNTIME.block_on(async {
            let catalog = MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
                .load(
                    "test",
                    HashMap::from([("warehouse".into(), "memory://warehouse".into())]),
                )
                .await
                .unwrap();
            let ns = NamespaceIdent::new("ns".into());
            catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
            let schema = Schema::builder()
                .with_fields(vec![NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::String),
                )
                .into()])
                .build()
                .unwrap();
            let spec = UnboundPartitionSpec::builder()
                .add_partition_field(1, "a", Transform::Identity)
                .unwrap()
                .build();
            let creation = TableCreation::builder()
                .name("t".into())
                .schema(schema)
                .partition_spec(spec)
                .build();
            let table = catalog.create_table(&ns, creation).await.unwrap();

            assert_eq!(
                write(&table, vec![], "p".into(), HashMap::new())
                    .await
                    .unwrap_err(),
                Error::new(
                    "feature_unsupported",
                    "writes to partitioned tables are not supported yet"
                )
            );
        });
    }
}
